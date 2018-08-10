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
    , Weight
      -- * Features
    , Features(..)
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

import SimplIR.Ranking as Ranking
import SimplIR.Ranking.Evaluation

type Score = Double

-- | Binary relevance judgement
data IsRelevant = NotRelevant | Relevant
                deriving (Ord, Eq, Show, Generic)
instance Hashable IsRelevant
instance NFData IsRelevant

newtype Features = Features { getFeatures :: VU.Vector Double }
                 deriving (Show, Eq)
                 deriving newtype NFData
type Weight = Features

featureDim :: Features -> Int
featureDim (Features v) = VU.length v

stepFeature :: Step -> Features -> Features
stepFeature (Step dim delta) (Features v) =
    Features $ VU.update v (VU.fromList [(dim, v VU.! dim + delta)])

-- | A ranking of documents along with relevance annotations
type FRanking relevance a = [(a, Features, relevance)]

dot :: Features -> Features -> Double
dot (Features v) (Features u) = VU.sum $ VU.zipWith (*) v u

-- | Re-rank a set of documents given a weight vector.
rerank :: Features -> [(a, Features)] -> Ranking Score a
rerank weight fRanking =
    Ranking.fromList
    [ (weight `dot` feats, doc)
    | (doc, feats) <- fRanking
    ]

data Step = Step Int Double

zeroStep :: Step
zeroStep = Step 0 0

isZeroStep :: Step -> Bool
isZeroStep (Step _ d) = d == 0

l2Normalize :: Features -> Features
l2Normalize (Features xs) = Features $ VU.map (/ norm) xs
  where norm = sqrt $ VU.sum $ VU.map squared xs

-- a few helpful utilities
squared :: Num a => a -> a
squared x = x*x

-- | @dotStepOracle w f step == (w + step) `dot` f@.
-- produces scorers that make use of the original dot product - only applying local modifications induced by the step
scoreStepOracle :: Weight -> Features -> (Step -> Score)
scoreStepOracle w@(Features w') f@(Features f') = scoreFun
  where
    scoreFun :: Step -> Score
    scoreFun s | isZeroStep s = score0
    scoreFun (Step dim delta) = score0 - scoreTerm dim 0 + scoreTerm dim delta

    -- scoreDeltaTerm: computes term contribution to dot product of w' + step
    scoreTerm dim off = (off + w' VU.! dim) * (f' VU.! dim)
    !score0 = w `dot` f

coordAscent :: forall a qid relevance gen. (Random.RandomGen gen, Show qid, Show a)
            => gen
            -> ScoringMetric relevance qid a
            -> Features -- ^ initial weights
            -> M.Map qid (FRanking relevance a)
            -> [(Score, Weight)]
coordAscent gen0 scoreRanking w0 fRankings
  | Just msg <- badMsg       = error msg
  | otherwise = go gen0 (score0, w0)
  where
    score0 = scoreRanking $ fmap (rerank w0 . map (\(doc, feats, rel) -> ((doc, rel), feats))) fRankings
    dim = featureDim w0

    -- lightweight checking of inputs
    badMsg | any (any $ \(_, fv, _) -> featureDim fv /= dim) fRankings -- check that features have same dimension as dim
             =  Just $ "Based on initial weights, Feature dimension expected to be "++show dim++ ", but feature vectors contain other dimensions. Examples "++
                         (intercalate "\n" $ take 10
                                           $ [ "("++show key ++", "++ show doc ++ ", featureDim = " ++ show (featureDim fv) ++ ") :" ++  show fv
                                             | (key, list) <-  M.toList fRankings
                                             , (doc, fv, _) <- list
                                             , featureDim fv /= dim
                                             ])
           | otherwise = Nothing

    deltas = [ f x
             | x <- [0.0001 * 2^n | n <- [1..25::Int]]
             , f <- [id, negate]
             ] ++ [0]

    go :: gen -> (Score, Weight) -> [(Score, Weight)]
    go gen w = w' : go gen' w'
      where
        w' = foldl' updateDim w dims
        dims = shuffle' [0..dim-1] dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, Weight) -> Int -> (Score, Weight)
    updateDim (_, w) dim =
        maximumBy (comparing fst)
        [ (scoreStep step, l2Normalize $ stepFeature step w)  -- possibly l2Normalize  requires to invalidate scoredRaking caches and a full recomputation of the scores
        | delta <- deltas
        , let step = Step dim delta
        ]
      where
        scoreStep :: Step -> Score
        scoreStep step =
            scoreRanking $ fmap (Ranking.fromList . map newScorer') cachedScoredRankings
          where
            newScorer' ::  (a, Step -> Score, relevance) -> (Score, (a, relevance))
            newScorer' (doc,scorer,rel) = (scorer step, (doc, rel))

        cachedScoredRankings :: M.Map qid [(a, Step -> Score, relevance)]
        cachedScoredRankings =
            fmap newScorer fRankings
          where
            newScorer :: FRanking relevance a -> [(a, Step -> Score, relevance)]
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
