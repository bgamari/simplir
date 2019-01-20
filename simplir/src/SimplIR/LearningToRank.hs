{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE RankNTypes #-}

{-# OPTIONS_GHC -Wno-name-shadowing #-}

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
    , naiveCoordAscent, naiveCoordAscent'
    , miniBatched
    , miniBatchedAndEvaluated
    , defaultMiniBatchParams, MiniBatchParams(..)
      -- * Helpers
    , IsRelevant(..)
    ) where

import GHC.Generics
import Control.DeepSeq
import Control.Parallel.Strategies
import Data.Ord
import Data.List
import qualified System.Random as Random
import System.Random.Shuffle
import Data.Hashable
import qualified Data.Map.Strict as M
import Linear.Epsilon

import qualified SimplIR.FeatureSpace as FS
import SimplIR.FeatureSpace (FeatureVec)
import SimplIR.Types.Relevance
import SimplIR.Ranking as Ranking
import SimplIR.Ranking.Evaluation
import SimplIR.TrainUtils
import Debug.Trace as Debug

type Score = Double

newtype WeightVec f s = WeightVec { getWeightVec :: FeatureVec f s Double }
                    deriving (Show, Generic, NFData)

-- | Returns 'Nothing' if vector is near zero magnitude.
l2NormalizeWeightVec :: WeightVec f s -> Maybe (WeightVec f s)
l2NormalizeWeightVec (WeightVec w)
  | nearZero norm = Nothing
  | otherwise     = Just $ WeightVec $ recip norm `FS.scale` w
  where norm = FS.l2Norm w

stepFeature :: Step s -> WeightVec f s -> WeightVec f s
stepFeature (Step dim delta) (WeightVec v) =
    let x = v `FS.lookupIndex` dim
        !x' = x + delta
    in WeightVec $ v `FS.modifyIndices` [(dim, x')]

-- | All necessary data to produce a ranking of documents (along with relevance annotations), given the weight parameter
type FRanking f s relevance a = [(a, FeatureVec f s Double, relevance)]


---
-- type FRanking f relevance a = [(a, relevance) , (FeatureVec f Double)]
-- type FRanking f relevance a = collection (a, relevance)  (FeatureVec f Double)

-- M.Map a (FeatureVec f Double, relevance)
--
-- rerank :: WeightVec f -> M.Map a (FeatureVec f Double) -> Ranking Score a
-- rerank :: WeightVec f -> Graph a (FeatureVec f Double) -> Ranking Score a
-- rerank :: WeightVec f -> collection a (FeatureVec f Double) -> Ranking Score a
--
---

score :: WeightVec f s -> FeatureVec f s Double -> Score
score (WeightVec w) f = w `FS.dot` f

-- | Re-rank a set of documents given a weight vector.
rerank :: WeightVec f s -> [(a, FeatureVec f s Double)] -> Ranking Score a
rerank weight fRanking =
    Ranking.fromList
    [ (weight `score` feats, doc)
    | (doc, feats) <- fRanking
    ]

data Step s = Step !(FS.FeatureIndex s) Double
     deriving Show

isZeroStep :: Step s -> Bool
isZeroStep (Step _ d) = d == 0

-- | @dotStepOracle w f step == (w + step) `dot` f@.
-- produces scorers that make use of the original dot product - only applying local modifications induced by the step
scoreStepOracle :: forall f s. WeightVec f s -> FeatureVec f s Double -> (Step s -> Score)
scoreStepOracle w f = scoreFun
  where
    scoreFun :: Step s -> Score
    scoreFun s | isZeroStep s = score0
    scoreFun (Step dim delta) = score0 - scoreTerm dim 0 + scoreTerm dim delta

    -- scoreDeltaTerm: computes term contribution to dot product of w' + step
    scoreTerm dim off = (off + getWeightVec w `FS.lookupIndex` dim) * (f `FS.lookupIndex` dim)
    !score0 = w `score` f

data MiniBatchParams = MiniBatchParams { miniBatchParamsBatchSteps :: Int   -- ^ iterations per mini-batch
                                       , miniBatchParamsBatchSize :: Int    -- ^ mini-batch size
                                       , miniBatchParamsEvalSteps :: Int    -- ^ mini-batches per evaluation cycle
                                       }
                                       deriving Show
defaultMiniBatchParams :: MiniBatchParams
defaultMiniBatchParams = MiniBatchParams 4 100 10

miniBatchedAndEvaluated
    :: forall a f s qid relevance gen.
       (Random.RandomGen gen, Show qid, Ord qid, Show a, Show relevance, Show f)
    => MiniBatchParams
    -> ScoringMetric relevance qid
       -- ^ evaluation metric
    -> (gen -> WeightVec f s -> M.Map qid (FRanking f s relevance a) -> [WeightVec f s])
       -- ^ optimiser (e.g. 'coordAscent')
    -> gen
    -> WeightVec f s  -- ^ initial weights
    -> M.Map qid (FRanking f s relevance a)
    -> [(Score, WeightVec f s)]
       -- ^ list of evaluation iterates with evaluation metric
miniBatchedAndEvaluated (MiniBatchParams batchSteps batchSize evalSteps) evalMetric
                        optimise gen00 w00 fRankings =
    go $ miniBatched batchSteps batchSize optimise gen00 w00 fRankings
  where
    -- shuffle tuples for 'rerank'
    fRankings' = fmap (map (\(doc,feats,rel) -> ((doc,rel),feats))) fRankings

    go :: [WeightVec f s] -> [(Score, WeightVec f s)]
    go iters =
        let w:rest = drop evalSteps iters

            rankings :: M.Map qid (Ranking Score (a, relevance))
            rankings = fmap (rerank w) fRankings'
        in (evalMetric rankings, w) : go rest


naiveCoordAscent
    :: forall a f s qid d gen relevance.
       (Random.RandomGen gen, Show qid, Show a, Show f)
    => ScoringMetric relevance qid
    -> (d -> WeightVec f s -> Ranking Double (a,relevance))
       -- ^ re-ranking function
    -> gen
    -> WeightVec f s           -- ^ initial weights
    -> M.Map qid d             -- ^ training data
    -> [(Score, WeightVec f s)]  -- ^ list of iterates
naiveCoordAscent scoreRanking rerank gen0 w0 fRankings =
    naiveCoordAscent' l2NormalizeWeightVec obj gen0 w0
  where
    obj w = scoreRanking $ withStrategy (parTraversable $ evalTraversable rseq) $ fmap (\d -> rerank d w) fRankings

deltas :: RealFrac a => [a]
deltas = [ f x
         | x <- [0.0001 * 2^n | n <- [1..20::Int]]
         , f <- [id, negate]
         ] ++ [0]

-- | Maximization via coordinate ascent.
naiveCoordAscent'
    :: forall f s gen.
       (Random.RandomGen gen, Show f)
    => (WeightVec f s -> Maybe (WeightVec f s)) -- ^ normalization
    -> (WeightVec f s -> Double)                -- ^ objective
    -> gen
    -> WeightVec f s                            -- ^ initial weights
    -> [(Double, WeightVec f s)]                -- ^ list of iterates
naiveCoordAscent' normalise obj gen0 w0
 | null (FS.featureIndexes fspace)  = error "naiveCoordAscent': Empty feature space"
 | otherwise =
    let Just w0' = normalise w0
    in go gen0 (obj w0', w0')
  where
    fspace = FS.featureSpace $ getWeightVec w0
    dim = FS.dimension fspace

    go :: gen -> (Score, WeightVec f s) -> [(Score, WeightVec f s)]
    go gen (s,w) = (s',w') : go gen' (s',w')
      where
        !(s', w') = foldl' updateDim (s,w) dims
        dims = shuffle' (FS.featureIndexes fspace) dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, WeightVec f s) -> FS.FeatureIndex s -> (Score, WeightVec f s)
    updateDim (score0, w0) dim
      | null steps = --Debug.trace ("coord dim "<> show dim<> " show score0 "<> show score0 <> ": No valid steps in ") $
                     (score0, w0)
      | otherwise  = --Debug.trace ("coord dim "<> show dim<> " show score0 "<> show score0 <> ": Found max: ") $
                    maximumBy (comparing fst) steps
      where
        steps :: [(Score, WeightVec f s)]
        steps =
            [ (score, w')
            | delta <- deltas
            , let step = Step dim delta
            , Just w' <- pure $ normalise $ stepFeature step w0
            , let score = obj w'
            , not $ isNaN score
            ]

coordAscent :: forall a f s qid relevance gen.
               (Random.RandomGen gen, Show qid, Show a, Show f)
            => ScoringMetric relevance qid
            -> gen
            -> WeightVec f s            -- ^ initial weights
            -> M.Map qid (FRanking f s relevance a)
            -> [(Score, WeightVec f s)] -- ^ list of iterates
coordAscent scoreRanking gen0 w0 fRankings
  | isNaN score0         = error "coordAscent: Initial score is not a number"
  | null (FS.featureIndexes fspace)  = error "coordAscent: Empty feature space"
  | otherwise = go gen0 (score0, w0')
  where
    Just w0' = l2NormalizeWeightVec w0
    score0 = scoreRanking $ fmap (rerank w0' . map (\(doc, feats, rel) -> ((doc, rel), feats))) fRankings

    fspace = FS.featureSpace $ getWeightVec w0
    dim = FS.dimension fspace

    zeroStep :: Step s
    zeroStep = Step (head $ FS.featureIndexes fspace) 0

    go :: gen -> (Score, WeightVec f s) -> [(Score, WeightVec f s)]
    go gen w = w' : go gen' w'
      where
        !w' = foldl' updateDim w dims
        dims = shuffle' (FS.featureIndexes fspace) dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, WeightVec f s) -> FS.FeatureIndex s -> (Score, WeightVec f s)
    updateDim (score0, w0) dim
      | null steps = (score0, w0)
      | otherwise  = maximumBy (comparing fst) steps
      where
        steps :: [(Score, WeightVec f s)]
        steps =
            [ (scoreStep step, w')  -- possibly l2Normalize  requires to invalidate scoredRaking caches and a full recomputation of the scores
            | delta <- deltas
            , let step = Step dim delta
            , Just w' <- pure $ l2NormalizeWeightVec $ stepFeature step w0
            ]
        scoreStep :: Step s -> Score
        scoreStep step
          | isNaN s   = error "coordAscent: Score is NaN"
          | otherwise = s
          where
            s = scoreRanking $ fmap (Ranking.fromList . fmap newScorer') cachedScoredRankings
            newScorer' ::  (a, Step s -> Score, relevance) -> (Score, (a, relevance))
            newScorer' (doc,scorer,rel) = (scorer step, (doc, rel))

        cachedScoredRankings :: M.Map qid [(a, Step s -> Score, relevance)]
        cachedScoredRankings =
            fmap newScorer fRankings
          where
            newScorer :: FRanking f s relevance a -> [(a, Step s -> Score, relevance)]
            newScorer = docSorted . fmap (middle (\f -> scoreStepOracle w0 f))

            docSorted = sortBy (comparing $ \(_,scoreFun,_) -> Down $ scoreFun zeroStep)

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
