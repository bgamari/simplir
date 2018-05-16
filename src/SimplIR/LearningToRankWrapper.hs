{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.LearningToRankWrapper where

import GHC.Generics

import Data.Aeson as Aeson
import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.Vector.Unboxed as VU
import Data.Maybe
import System.Random

import SimplIR.LearningToRank
import qualified SimplIR.Format.TrecRunFile as Run
import qualified SimplIR.Format.QRel as QRel

data Model = Model { modelWeights :: M.Map FeatureName Double
                   }
           deriving (Show, Generic)
instance ToJSON Model
instance FromJSON Model


newtype FeatureName = FeatureName { getFeatureName :: T.Text }
                    deriving (Ord, Eq, Show, ToJSON, FromJSON, ToJSONKey, FromJSONKey)


runToDocFeatures :: M.Map FeatureName [Run.RankingEntry]
                 -> M.Map (Run.QueryId, QRel.DocumentName) (M.Map FeatureName Double)
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


toDocFeatures' :: [FeatureName]
              -> M.Map FeatureName [Run.RankingEntry]
              -> M.Map (Run.QueryId, QRel.DocumentName) Features
toDocFeatures' allFeatures runFiles =
    fmap (toFeatures' allFeatures) (runToDocFeatures runFiles)

-- toFeatures :: S.Set FeatureName -> M.Map FeatureName Double -> Features
-- toFeatures allFeatures features =
--     Features $ VU.fromList [ features M.! f | f <- S.toList allFeatures ]
--
toFeatures' :: [FeatureName] -> M.Map FeatureName Double -> Features
toFeatures' allFeatures features =
    Features $ VU.fromList [ features M.! f | f <- allFeatures ]


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


avgMetricData :: forall query doc. (Ord query)
              => M.Map query [(doc, Features, IsRelevant)]
              -> ScoringMetric IsRelevant query doc
avgMetricData traindata =
    let totalRel = fmap (length . filter (\(_,_, rel)-> (rel == Relevant)) )  traindata
        metric :: ScoringMetric IsRelevant query doc
        metric = meanAvgPrec (fromMaybe 0 . (`M.lookup` totalRel)) Relevant
    in metric

augmentWithQrels :: forall docId queryId. (Ord queryId, Ord docId)
                 => [QRel.Entry queryId docId IsRelevant]
                 -> M.Map (queryId, docId) Features
                 -> IsRelevant
                 -> M.Map queryId [(docId, Features, IsRelevant)]
augmentWithQrels qrel docFeatures rel=
    let relevance :: M.Map (queryId, docId) IsRelevant
        relevance = M.fromList [ ((qid, doc), rel) | QRel.Entry qid doc rel <- qrel ]

        franking :: M.Map queryId [(docId, Features, IsRelevant)]
        franking = M.fromListWith (++)
                   [ (qid, [(doc, features, rel)])
                   | ((qid, doc), features) <- M.assocs docFeatures
                   , let rel = M.findWithDefault NotRelevant (qid, doc) relevance
                   ]
    in franking

learnToRank :: (Ord query, Show query, Show docId)
            => M.Map query [(docId, Features, IsRelevant)]
            -> [FeatureName]
            -> ScoringMetric IsRelevant query docId
            -> StdGen
            -> (Model, Double)
learnToRank franking featureNames metric gen0 =
    let weights0 :: Features
        weights0 = Features $ VU.replicate (length featureNames) 1
        iters = coordAscent gen0 metric weights0 franking
        errorDiag = (show weights0) ++ ". Size training queries: "++ (show $ M.size (franking))++ "."
        hasConverged (a,_) (b,_)
           | isNaN b = error $ "Metric score is NaN. initial weights " ++ errorDiag
           | otherwise = (abs (a-b))/(abs b) < 1e-3
        convergence = untilConverged hasConverged
        (evalScore, Features weights) = case convergence iters of
           []          -> error $ "learning converged immediately. "++errorDiag
           itersResult -> last itersResult
        modelWeights_ = M.fromList $ zip featureNames (VU.toList weights)
    in (Model {modelWeights = modelWeights_}, evalScore)


rerankRankings :: Model
         -> M.Map Run.QueryId [(QRel.DocumentName, Features)]
         -> M.Map Run.QueryId (Ranking Score QRel.DocumentName)
rerankRankings model featureData  =
    fmap (rerank (toWeights model)) featureData



-- type Rankings rel qid doc = M.Map qid (Ranking (doc, rel))
  -- Ranking a  -> Ranking (doc, rel)

rerankRankings' :: Model
         -> M.Map q [(docId, Features, rel)]
         -> M.Map q (Ranking Score (docId, rel))
rerankRankings' model featureData  =
    fmap (rerank (toWeights model))
    $ fmap rearrangeTuples featureData
  where rearrangeTuples = (fmap (\(d,f,r)-> ((d,r),f)))



toWeights :: Model -> Features
toWeights (Model weights) =
    Features $ VU.fromList $ M.elems weights

untilConverged :: (a -> a -> Bool) -> [a] -> [a]
untilConverged eq xs0 = go xs0
  where
    go (x:y:_)
      | x `eq` y  = x : y : []
    go (x:rest)   = x : go rest
    go []         = []
