{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
    let totalRel = M.fromListWith (+) [ (qid, n)
                                      | QRel.Entry qid _ rel <- qrel
                                      , let n = case rel of Relevant -> 1
                                                            NotRelevant -> 0 ]
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
                   [ (qid, [(doc, features, rel)])
                   | ((qid, doc), features) <- M.assocs docFeatures
                   , let rel = M.findWithDefault NotRelevant (qid, doc) relevance
                   ]
    in franking

learnToRank :: forall f query docId. (Ord query, Show query, Show docId, Show f)
            => M.Map query [(docId, FeatureVec f Double, IsRelevant)]
            -> FeatureSpace f
            -> ScoringMetric IsRelevant query docId
            -> StdGen
            -> (Model f, Double)
learnToRank franking fspace metric gen0 =
    let weights0 :: WeightVec f
        weights0 = WeightVec $ FS.repeat fspace 1 --  $ VU.replicate (length featureNames) 1
        iters = miniBatchedAndEvaluated 4 25 10
            metric (coordAscent metric) gen0 weights0 franking
        --iters = coordAscent metric gen0 weights0
        --    (fmap (\xs -> [(a, b, c) | (a, b, c) <- xs]) franking)
        errorDiag = show weights0 ++ ". Size training queries: "++ show (M.size franking)++ "."
        hasConverged (a,_) (b,_)
           | isNaN b = error $ "Metric score is NaN. initial weights " ++ errorDiag
           | otherwise = relChange a b < 1e-2
        convergence = untilConverged hasConverged . traceIters
        (evalScore, weights) = case convergence iters of
           []          -> error $ "learning converged immediately. "++errorDiag
           itersResult -> last itersResult
    in (Model weights, evalScore)

traceIters :: [(Double, a)] -> [(Double, a)]
traceIters xs = zipWith3 f [1..] xs (tail xs)
  where
    f i x@(a,_) (b,_) =
        trace (concat ["iteration ", show i, ", score ", show a, " -> ", show b, "rel ", show (relChange a b)]) x

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
    fmap (rerank (modelWeights' model))
    $ fmap rearrangeTuples featureData
  where rearrangeTuples = (fmap (\(d,f,r)-> ((d,r), f)))

untilConverged :: (a -> a -> Bool) -> [a] -> [a]
untilConverged eq xs0 = go xs0
  where
    go (x:y:_)
      | x `eq` y  = x : y : []
    go (x:rest)   = x : go rest
    go []         = []
