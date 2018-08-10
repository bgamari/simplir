{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}

module SimplIR.LearningToRank
    ( -- * Basic types
      Score
    , Ranking
    , TotalRel
    , WeightVec(..)
    , score
      -- * Features
      -- * Computing rankings
    , rerank
      -- * Scoring metrics
    , ScoringMetric
    , meanAvgPrec
      -- * Learning
    , FRanking
    , coordAscent
      -- * Helpers
    , IsRelevant(..)
    ) where

import GHC.Generics
import Control.DeepSeq
import Data.Ord
import Data.List
import qualified System.Random as Random
import System.Random.Shuffle
import Data.Hashable
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as VU

import SimplIR.FeatureSpace as FS
import SimplIR.Ranking as Ranking
import SimplIR.Ranking.Evaluation

type Score = Double

newtype WeightVec f = WeightVec { getWeightVec :: FeatureVec f Double }
                    deriving (Show, Generic, NFData)

l2NormalizeWeightVec :: WeightVec f -> WeightVec f
l2NormalizeWeightVec = WeightVec . FS.l2Normalize . getWeightVec

-- | Binary relevance judgement
data IsRelevant = NotRelevant | Relevant
                deriving (Ord, Eq, Show, Generic)
instance Hashable IsRelevant
instance NFData IsRelevant

stepFeature :: Step f -> WeightVec f -> WeightVec f
stepFeature (Step dim delta) (WeightVec v) =
    WeightVec $ unsafeFeatureVecFromVector
    $ VU.update (getFeatureVec v) (VU.fromList [(getFeatureIndex dim, v `FS.lookupIndex` dim + delta)])

-- | A ranking of documents along with relevance annotations
type FRanking f relevance a = [(a, FeatureVec f Double, relevance)]

score :: WeightVec f -> FeatureVec f Double -> Score
score (WeightVec w) f = w `FS.dotFeatureVecs` f

-- | Re-rank a set of documents given a weight vector.
rerank :: WeightVec f -> [(a, FeatureVec f Double)] -> Ranking Score a
rerank weight fRanking =
    Ranking.fromList
    [ (weight `score` feats, doc)
    | (doc, feats) <- fRanking
    ]

data Step f = Step !(FeatureIndex f) Double

isZeroStep :: Step f -> Bool
isZeroStep (Step _ d) = d == 0

-- | @dotStepOracle w f step == (w + step) `dot` f@.
-- produces scorers that make use of the original dot product - only applying local modifications induced by the step
scoreStepOracle :: forall f. WeightVec f -> FeatureVec f Double -> (Step f -> Score)
scoreStepOracle w f = scoreFun
  where
    scoreFun :: Step f -> Score
    scoreFun s | isZeroStep s = score0
    scoreFun (Step dim delta) = score0 - scoreTerm dim 0 + scoreTerm dim delta

    -- scoreDeltaTerm: computes term contribution to dot product of w' + step
    scoreTerm dim off = (off + getWeightVec w `FS.lookupIndex` dim) * (f `FS.lookupIndex` dim)
    !score0 = w `score` f

coordAscent :: forall a f qid relevance gen.
               (Random.RandomGen gen, Show qid, Show a)
            => gen
            -> ScoringMetric relevance qid a
            -> FeatureSpace f
            -> WeightVec f   -- ^ initial weights
            -> M.Map qid (FRanking f relevance a)
            -> [(Score, WeightVec f)]
coordAscent gen0 scoreRanking fspace w0 fRankings
  | Just msg <- badMsg       = error msg
  | otherwise = go gen0 (score0, w0)
  where
    score0 = scoreRanking $ fmap (rerank w0 . map (\(doc, feats, rel) -> ((doc, rel), feats))) fRankings

    dim = FS.featureDimension fspace

    zeroStep :: Step f
    zeroStep = Step (head $ featureIndexes fspace) 0

    -- lightweight checking of inputs
    badMsg | any (any $ \(_, fv, _) -> featureVecDimension fv /= dim) fRankings -- check that features have same dimension as dim
             =  Just $ "Based on initial weights, Feature dimension expected to be "++show dim++ ", but feature vectors contain other dimensions. Examples "++
                         (intercalate "\n" $ take 10
                                           $ [ "("++show key ++", "++ show doc ++ ", featureDim = " ++ show (featureVecDimension fv) ++ ") :" ++  show fv
                                             | (key, list) <-  M.toList fRankings
                                             , (doc, fv, _) <- list
                                             , featureVecDimension fv /= dim
                                             ])
           | otherwise = Nothing

    deltas = [ f x
             | x <- [0.0001 * 2^n | n <- [1..25::Int]]
             , f <- [id, negate]
             ] ++ [0]

    go :: gen -> (Score, WeightVec f) -> [(Score, WeightVec f)]
    go gen w = w' : go gen' w'
      where
        w' = foldl' updateDim w dims
        dims = shuffle' (featureIndexes fspace) dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, WeightVec f) -> FeatureIndex f -> (Score, WeightVec f)
    updateDim (_, w) dim =
        maximumBy (comparing fst)
        [ (scoreStep step, l2NormalizeWeightVec $ stepFeature step w)  -- possibly l2Normalize  requires to invalidate scoredRaking caches and a full recomputation of the scores
        | delta <- deltas
        , let step = Step dim delta
        ]
      where
        scoreStep :: Step f -> Score
        scoreStep step =
            scoreRanking $ fmap (Ranking.fromList . map newScorer') cachedScoredRankings
          where
            newScorer' ::  (a, Step f -> Score, relevance) -> (Score, (a, relevance))
            newScorer' (doc,scorer,rel) = (scorer step, (doc, rel))

        cachedScoredRankings :: M.Map qid [(a, Step f -> Score, relevance)]
        cachedScoredRankings =
            fmap newScorer fRankings
          where
            newScorer :: FRanking f relevance a -> [(a, Step f -> Score, relevance)]
            newScorer = docSorted . map (middle (\f -> scoreStepOracle w f))

            docSorted = sortBy (flip $ comparing $ \(_,scoreFun,_) -> scoreFun zeroStep)

middle :: (b->b') -> (a, b, c) -> (a,b',c)
middle fun (a, b, c) = (a, fun b, c)

-----------------------
-- Testing

{-
type DocId = String
type TestRanking = Ranking (DocId, IsRelevant)

rankingA :: TestRanking
rankingA = Ranking
    [ aDoc "ben"     10 Relevant
    , aDoc "laura"   9 Relevant
    , aDoc "T!"      3 Relevant
    , aDoc "snowman" 4 NotRelevant
    , aDoc "cat"     5 NotRelevant
    ]

aDoc docid score rel = (score, (docid, rel))

rankingB :: TestRanking
rankingB = Ranking
    [ aDoc "ben" 3 NotRelevant
    , aDoc "laura" 9 Relevant
    , aDoc "snowman" 4 Relevant
    ]

rankingC :: TestRanking
rankingC = Ranking
    [ aDoc "cat" 9 Relevant
    , aDoc "T!" 10 Relevant
    ]

testFeatures :: M.Map Char (FRanking IsRelevant DocId)
testFeatures =
    fmap (map toFeatures) testRankings
  where
    toFeatures :: _
    toFeatures (a,b) = (a, Features $ VU.fromList [1,b])

testRankings :: M.Map Char TestRanking
testRankings =
    M.fromList $ zip ['a'..] [rankingA, rankingB, rankingC]
-}
