{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module SimplIR.LearningToRankWrapper
    ( Model(..)
    , SomeModel(..)
    , WeightVec(..)
    , modelWeights
    , modelFeatures
    , toFeatures'
    , toDocFeatures'
    , totalRelevantFromQRels
    , totalRelevantFromData
    , augmentWithQrels
    , learnToRank
    , rerankRankings
    , rerankRankings'
    , untilConverged
    , defaultConvergence, relChangeBelow, dropIterations, maxIterations, ConvergenceCriterion

    -- * Convenience
    , FeatureName(..)
    ) where

import GHC.Generics

import Control.DeepSeq
import Data.Bifunctor
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson (Parser)
import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.HashMap.Strict as HM
import Data.Maybe
import System.Random
import Debug.Trace

import SimplIR.LearningToRank
import SimplIR.FeatureSpace (FeatureSpace, FeatureVec)
import qualified SimplIR.FeatureSpace as FS
import qualified SimplIR.Format.TrecRunFile as Run
import qualified SimplIR.Format.QRel as QRel

type ConvergenceCriterion f s = ([(Double, WeightVec f s)] -> [(Double, WeightVec f s)])

data SomeModel f where
    SomeModel :: Model f s -> SomeModel f

newtype Model f s = Model { modelWeights' :: WeightVec f s }
                  deriving (Show, Generic)
instance NFData (Model f s) where rnf = (`seq` ())

modelWeights :: Model f s -> [(f, Double)]
modelWeights (Model weights) = FS.toList (getWeightVec weights)

modelFeatures :: Model f s -> FeatureSpace f s
modelFeatures = FS.featureSpace . getWeightVec . modelWeights'

-- TODO also save feature space to ensure features idx don't change semantics
instance (Ord f, Show f) => Aeson.ToJSON (Model f s) where
    toJSON (Model weights) =
        Aeson.toJSON $ M.fromList $ map (first show) $ FS.toList (getWeightVec weights)
    toEncoding (Model weights) =
        Aeson.pairs $ foldMap (\(f,x) -> T.pack (show f) Aeson..= x) (FS.toList (getWeightVec weights))
instance (Ord f, Show f, Read f) => Aeson.FromJSON (SomeModel f) where
    parseJSON = Aeson.withObject "feature vector" $ \o -> do
      let parsePair :: (T.Text, Aeson.Value) -> Aeson.Parser (f, Double)
          parsePair (k,v)
            | (k', "") : _ <- reads (T.unpack k) =
                  (,) <$> pure k' <*> Aeson.parseJSON v
            | otherwise = fail $ "Failed to parse feature name: "++show k
      pairs <- mapM parsePair (HM.toList o)
      let FS.SomeFeatureSpace fspace = FS.mkFeatureSpace $ S.fromList $ map fst pairs
          weights = WeightVec $ FS.fromList fspace pairs
      return $ SomeModel $ Model weights


newtype FeatureName = FeatureName { getFeatureName :: T.Text }
                    deriving (Ord, Eq, Show, Read, Aeson.ToJSON, Aeson.FromJSON, Aeson.ToJSONKey, Aeson.FromJSONKey)

runToDocFeatures :: Ord f
                 => M.Map f [Run.RankingEntry]
                 -> M.Map (Run.QueryId, QRel.DocumentName) (M.Map f Double)
runToDocFeatures runFiles = M.fromListWith M.union
            [ ( (Run.queryId entry, Run.documentName entry)
              , M.singleton featureName (Run.documentScore entry) )
            | (featureName, ranking) <- M.toList runFiles
            , entry <- ranking
            ]


-- LD: I don't like the assumption that the order in this set is stable.runToDocFeatures
-- The code goes between a list of feature names and a set back and forth, so this isn't even efficient
-- toDocFeatures :: S.Set FeatureName
--               -> M.Map FeatureName [Run.RankingEntry]
--               -> M.Map (Run.QueryId, QRel.DocumentName) Features
-- toDocFeatures allFeatures runFiles =
--     fmap (toFeatures allFeatures) (runToDocFeatures runFiles)


toDocFeatures' :: (Show f, Ord f)
               => FeatureSpace f s
               -> M.Map f [Run.RankingEntry]
               -> M.Map (Run.QueryId, QRel.DocumentName) (FeatureVec f s Double)
toDocFeatures' fspace runFiles =
    fmap (toFeatures' fspace) (runToDocFeatures runFiles)

toFeatures' :: (Show f, Ord f) => FeatureSpace f s -> M.Map f Double -> FeatureVec f s Double
toFeatures' fspace features =
    FS.fromList fspace [ (f, features M.! f) | f <- FS.featureNames fspace ]

-- | Compute a function for the total number of relevant documents for a set of
-- queries.
totalRelevantFromQRels :: Ord query
              => [QRel.Entry query doc IsRelevant]
              -> query -> TotalRel
totalRelevantFromQRels qrel =
    fromMaybe 0 . (`M.lookup` totalRel)
  where
    totalRel =
        M.fromListWith (+)
        [ (qid, n)
        | QRel.Entry qid _ rel <- qrel
        , let n = case rel of Relevant -> 1
                              NotRelevant -> 0
        ]

totalRelevantFromData
    :: forall query doc f s. (Ord query)
    => M.Map query [(doc, FeatureVec f s Double, IsRelevant)]
    -> query -> TotalRel
totalRelevantFromData trainData =
    fromMaybe 0 . (`M.lookup` totalRel)
  where
    totalRel = fmap (length . filter (\(_,_, rel)-> (rel == Relevant))) trainData

augmentWithQrels :: forall docId queryId f s.
                    (Ord queryId, Ord docId)
                 => [QRel.Entry queryId docId IsRelevant]
                 -> M.Map (queryId, docId) (FeatureVec f s Double)
                 -> M.Map queryId [(docId, FeatureVec f s Double, IsRelevant)]
augmentWithQrels qrel docFeatures =
    let relevance :: M.Map (queryId, docId) IsRelevant
        relevance = M.fromList [ ((qid, doc), rel)
                               | QRel.Entry qid doc rel <- qrel
                               ]

        franking :: M.Map queryId [(docId, FeatureVec f s Double, IsRelevant)]
        franking = M.fromListWith (++)
                   [ (qid, [(doc, features, relDocs)])
                   | ((qid, doc), features) <- M.assocs docFeatures
                   , let relDocs = M.findWithDefault NotRelevant (qid, doc) relevance
                   ]
    in franking



learnToRank :: forall f s query docId. (Ord query, Show query, Show docId, Show f)
            => MiniBatchParams
            -> ConvergenceCriterion f s
            -> EvalCutoff
            -> M.Map query [(docId, FeatureVec f s Double, IsRelevant)]
            -> FeatureSpace f s
            -> ScoringMetric IsRelevant query
            -> StdGen
            -> (Model f s, Double)
learnToRank miniBatchParams convergence evalCutoff franking fspace metric gen0 =
    let weights0 :: WeightVec f s
        weights0 = WeightVec $ FS.repeat fspace 1
        iters =
            let optimise gen w trainData =
                    map snd $ coordAscent evalCutoff metric gen w trainData
            in miniBatchedAndEvaluated miniBatchParams
                 metric optimise gen0 weights0 franking
        errorDiag = show weights0 ++ ". Size training queries: "++ show (M.size franking)++ "."
        checkNans (_,_) (b,_)
           | isNaN b = error $ "Metric score is NaN. initial weights " ++ errorDiag
           | otherwise = True
        checkedConvergence :: [(Double, WeightVec f s)] -> [(Double, WeightVec f s)]
        checkedConvergence = untilConverged checkNans . convergence
        (evalScore, weights) = case checkedConvergence iters of
           []          -> error $ "learning converged immediately. "++errorDiag
           itersResult -> last itersResult
    in (Model weights, evalScore)

traceIters :: String -> [(Double, a)] -> [(Double, a)]
traceIters info xs = zipWith3 g [1 :: Integer ..] xs (tail xs)
  where
    g i x@(a,_) (b,_) =
        trace (concat [info, " iteration ", show i, ", score ", show a, " -> ", show b, " rel ", show (relChange a b)]) x

defaultConvergence :: String -> Double -> Int -> Int -> [(Double, WeightVec f s)] -> [(Double, WeightVec f s)]
defaultConvergence info threshold maxIter dropIter =
    relChangeBelow threshold . maxIterations maxIter . dropIterations dropIter . traceIters info



relChange :: RealFrac a => a -> a -> a
relChange a b = abs (a-b) / abs b

rerankRankings :: Model f s
               -> M.Map Run.QueryId [(QRel.DocumentName, FeatureVec f s Double)]
               -> M.Map Run.QueryId (Ranking Score QRel.DocumentName)
rerankRankings model featureData  =
    fmap (rerank (modelWeights' model)) featureData

rerankRankings' :: Model f s
                -> M.Map q [(docId, FeatureVec f s Double, rel)]
                -> M.Map q (Ranking Score (docId, rel))
rerankRankings' model featureData  =
    fmap (rerank (modelWeights' model) . rearrangeTuples) featureData
  where rearrangeTuples = (fmap (\(d,f,r)-> ((d,r), f)))

untilConverged :: (a -> a -> Bool) -> [a] -> [a]
untilConverged conv xs0 = go xs0
  where
    go (x:y:_)
      | x `conv` y  = [x, y]
    go (x:rest)     = x : go rest
    go []           = []

maxIterations :: Int -> [a] -> [a]
maxIterations = take
dropIterations :: Int -> [a] -> [a]
dropIterations = drop

relChangeBelow :: Double -> [(Double, b)] -> [(Double, b)]
relChangeBelow threshold xs =
    untilConverged (\(x,_) (y,_) -> relChange x y < threshold) xs
