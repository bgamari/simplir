{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module SimplIR.LearningToRankWrapper
    ( Model(..)
    , WeightVec(..)
    , modelWeights
    , modelFeatures
    , toFeatures'
    , toDocFeatures'
    , avgMetricQrel
    , avgMetricData
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
import qualified Data.HashMap.Strict as HM
import Data.Maybe
import System.Random
import Debug.Trace

import SimplIR.LearningToRank
import SimplIR.FeatureSpace
import qualified SimplIR.FeatureSpace as FS
import qualified SimplIR.Format.TrecRunFile as Run
import qualified SimplIR.Format.QRel as QRel

type ConvergenceCriterion f = ([(Double, WeightVec f)] -> [(Double, WeightVec f)])

newtype Model f = Model { modelWeights' :: WeightVec f }
                deriving (Show, Generic)
instance NFData (Model f) where rnf = (`seq` ())

modelWeights :: Model f -> [(f, Double)]
modelWeights (Model weights) = FS.toList (getWeightVec weights)

modelFeatures :: Model f -> FeatureSpace f
modelFeatures = featureSpace . getWeightVec . modelWeights'

-- TODO also save feature space to ensure features idx don't change semantics
instance (Ord f, Show f) => Aeson.ToJSON (Model f) where
    toJSON (Model weights) =
        Aeson.toJSON $ M.fromList $ map (first show) $ FS.toList (getWeightVec weights)
    toEncoding (Model weights) =
        Aeson.pairs $ foldMap (\(f,x) -> T.pack (show f) Aeson..= x) (FS.toList (getWeightVec weights))
instance (Ord f, Show f, Read f) => Aeson.FromJSON (Model f) where
    parseJSON = Aeson.withObject "feature vector" $ \o -> do
      let parsePair :: (T.Text, Aeson.Value) -> Aeson.Parser (f, Double)
          parsePair (k,v)
            | (k', "") : _ <- reads (T.unpack k) =
                  (,) <$> pure k' <*> Aeson.parseJSON v
            | otherwise = fail $ "Failed to parse feature name: "++show k
      pairs <- mapM parsePair (HM.toList o)
      let fspace = FS.mkFeatureSpace $ map fst pairs
          weights = WeightVec $ FS.fromList fspace pairs
      return $ Model weights


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
               => FeatureSpace f
               -> M.Map f [Run.RankingEntry]
               -> M.Map (Run.QueryId, QRel.DocumentName) (FeatureVec f Double)
toDocFeatures' fspace runFiles =
    fmap (toFeatures' fspace) (runToDocFeatures runFiles)

toFeatures' :: (Show f, Ord f) => FeatureSpace f -> M.Map f Double -> FeatureVec f Double
toFeatures' fspace features =
    FS.fromList fspace [ (f, features M.! f) | f <- featureNames fspace ]


avgMetricQrel :: forall query doc. Ord query
              => [QRel.Entry query doc IsRelevant]
              -> ScoringMetric IsRelevant query doc
avgMetricQrel qrel =
    let !totalRel = M.fromListWith (+)
                      [ (qid, n)
                      | QRel.Entry qid _ rel <- qrel
                      , let n = case rel of Relevant -> 1
                                            NotRelevant -> 0
                      ]
        metric :: ScoringMetric IsRelevant query doc
        metric = meanAvgPrec (fromMaybe 0 . (`M.lookup` totalRel)) Relevant
    in metric


avgMetricData :: forall query doc f. (Ord query)
              => M.Map query [(doc, FeatureVec f Double, IsRelevant)]
              -> ScoringMetric IsRelevant query doc
avgMetricData traindata =
    let totalRel = fmap (length . filter (\(_,_, rel)-> (rel == Relevant)) )  traindata
        metric :: ScoringMetric IsRelevant query doc
        metric = meanAvgPrec (fromMaybe 0 . (`M.lookup` totalRel)) Relevant
    in metric

augmentWithQrels :: forall docId queryId f.
                    (Ord queryId, Ord docId)
                 => [QRel.Entry queryId docId IsRelevant]
                 -> M.Map (queryId, docId) (FeatureVec f Double)
                 -> IsRelevant
                 -> M.Map queryId [(docId, FeatureVec f Double, IsRelevant)]
augmentWithQrels qrel docFeatures rel=
    let relevance :: M.Map (queryId, docId) IsRelevant
        relevance = M.fromList [ ((qid, doc), rel) | QRel.Entry qid doc rel <- qrel ]

        franking :: M.Map queryId [(docId, FeatureVec f Double, IsRelevant)]
        franking = M.fromListWith (++)
                   [ (qid, [(doc, features, relDocs)])
                   | ((qid, doc), features) <- M.assocs docFeatures
                   , let relDocs = M.findWithDefault NotRelevant (qid, doc) relevance
                   ]
    in franking



learnToRank :: forall f query docId . (Ord query, Show query, Show docId, Show f)
            => MiniBatchParams
            -> ConvergenceCriterion f
            -> M.Map query [(docId, FeatureVec f Double, IsRelevant)]
            -> FeatureSpace f
            -> ScoringMetric IsRelevant query docId
            -> StdGen
            -> (Model f, Double)
learnToRank miniBatchParams convergence franking fspace metric gen0 =
    let weights0 :: WeightVec f
        weights0 = WeightVec $ FS.repeat fspace 1
        iters =
            let optimise gen w trainData =
                    map snd $ coordAscent metric gen w trainData
            in miniBatchedAndEvaluated miniBatchParams
                 metric optimise gen0 weights0 franking
        errorDiag = show weights0 ++ ". Size training queries: "++ show (M.size franking)++ "."
        checkNans (_,_) (b,_)
           | isNaN b = error $ "Metric score is NaN. initial weights " ++ errorDiag
           | otherwise = True
        checkedConvergence :: [(Double, WeightVec f)] -> [(Double, WeightVec f)]
        checkedConvergence = untilConverged checkNans . convergence
        (evalScore, weights) = case checkedConvergence iters of
           []          -> error $ "learning converged immediately. "++errorDiag
           itersResult -> last itersResult
    in (Model weights, evalScore)

traceIters :: String -> [(Double, a)] -> [(Double, a)]
traceIters info xs = zipWith3 g [1..] xs (tail xs)
  where
    g i x@(a,_) (b,_) =
        trace (concat [info, " iteration ", show i, ", score ", show a, " -> ", show b, " rel ", show (relChange a b)]) x

defaultConvergence :: String -> Double -> Int -> Int -> [(Double, WeightVec f)] -> [(Double, WeightVec f)]
defaultConvergence info threshold maxIter dropIter =
    relChangeBelow threshold . maxIterations maxIter . dropIterations dropIter . traceIters info



relChange :: RealFrac a => a -> a -> a
relChange a b = abs (a-b) / abs b

rerankRankings :: Model f
               -> M.Map Run.QueryId [(QRel.DocumentName, FeatureVec f Double)]
               -> M.Map Run.QueryId (Ranking Score QRel.DocumentName)
rerankRankings model featureData  =
    fmap (rerank (modelWeights' model)) featureData

rerankRankings' :: Model f
         -> M.Map q [(docId, FeatureVec f Double, rel)]
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
