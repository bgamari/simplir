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
      -- ** Coordinate ascent optimisation
    , coordAscent
    , EvalCutoff(..)
    , naiveCoordAscent, naiveCoordAscent'
      -- ** Mini-batching
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
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as VU
import Linear.Epsilon

import qualified SimplIR.FeatureSpace as FS
import SimplIR.FeatureSpace (FeatureVec)
import SimplIR.Types.Relevance
import SimplIR.Ranking as Ranking
import SimplIR.Ranking.Evaluation
import SimplIR.TrainUtils

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
defaultMiniBatchParams = MiniBatchParams 1 100 0

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

-- | 'coordAscent' can optionally truncate rankings during evaluation. This
-- improves runtime complexity at the cost of a slight approximation.
data EvalCutoff = EvalNoCutoff
                | EvalCutoffAt !Int

coordAscent :: forall a f s qid relevance gen.
               (Random.RandomGen gen, Show qid, Show a, Show f)
            => EvalCutoff               -- ^ ranking truncation point for scoring
            -> ScoringMetric relevance qid
            -> gen
            -> WeightVec f s            -- ^ initial weights
            -> M.Map qid (FRanking f s relevance a)
            -> [(Score, WeightVec f s)] -- ^ list of iterates
coordAscent evalCutoff scoreRanking gen0 w0
  | null (FS.featureIndexes fspace)  = error "coordAscent: Empty feature space"
  | otherwise = \fRankings ->
      let Just w0' = l2NormalizeWeightVec w0
          score0 = scoreRanking fRankings'
          fRankings' = fmap (rerank w0' . map (\(doc, feats, rel) -> (((doc, feats), rel), feats))) fRankings
      in if isNaN score0
         then error "coordAscent: Initial score is not a number"
         else go gen0 (score0, w0', fRankings')
  where
    fspace = FS.featureSpace $ getWeightVec w0
    dim = FS.dimension fspace

    zeroStep :: Step s
    zeroStep = Step (head $ FS.featureIndexes fspace) 0

    go :: gen
       -> (Score, WeightVec f s, M.Map qid (Ranking Score ((a, FeatureVec f s Double), relevance)))
       -> [(Score, WeightVec f s)]
    go gen state = (score, w') : go gen' state'
      where
        state'@(score, !w', _fRankings') = foldl' updateDim state dims
        dims = shuffle' (FS.featureIndexes fspace) dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, WeightVec f s, M.Map qid (Ranking Score ((a, FeatureVec f s Double), relevance)))
              -> FS.FeatureIndex s
              -> (Score, WeightVec f s, M.Map qid (Ranking Score ((a, FeatureVec f s Double), relevance)))
    updateDim (score0, w0, fRankings) dim
      | null steps = (score0, w0, fRankings)
      | otherwise  =
          let (score, step) = maximumBy (comparing fst) steps
              Just w = l2NormalizeWeightVec $ stepFeature step w0
          in (score, w, updateRankings (stepScorerRankings EvalNoCutoff) step)
      where
        steps :: [(Score, Step s)]
        steps =
            [ if isNaN score
              then error "coordAscent: Score is NaN"
              else (score, step)
              -- possibly l2Normalize requires to invalidate scoredRaking caches
              -- and a full recomputation of the scores
            | delta <- deltas
            , let step = Step dim delta
            , Just _ <- pure $ l2NormalizeWeightVec $ stepFeature step w0
            , let !rankings = updateRankings cachedScoredRankings step
            , let score = scoreRanking rankings
            ]

        updateRankings :: M.Map qid (Ranking Score (Step s -> Score, ((a, FeatureVec f s Double), relevance)))
                       -> Step s
                       -> M.Map qid (Ranking Score ((a, FeatureVec f s Double), relevance))
        updateRankings rankings step =
            fmap (\ranking -> rescore scoreDoc ranking) rankings
          where
            scoreDoc :: (Step s -> Score, ((a, FeatureVec f s Double), relevance))
                     -> (Score, ((a, FeatureVec f s Double), relevance))
            scoreDoc (stepScorer, item) = (stepScorer step, item)

        !cachedScoredRankings = stepScorerRankings evalCutoff

        mapEvalRanking :: forall a b a' b'. (VU.Unbox a, VU.Unbox a', Ord a')
                       => EvalCutoff
                       -> (a -> b -> (a', b'))
                       -> Ranking a b -> Ranking a' b'
        mapEvalRanking EvalNoCutoff = mapRanking
        mapEvalRanking (EvalCutoffAt k) = mapRankingK k

        stepScorerRankings :: EvalCutoff
                           -> M.Map qid (Ranking Score (Step s -> Score, ((a, FeatureVec f s Double), relevance)))
        stepScorerRankings cutoff =
            fmap augment fRankings
          where
            -- Attach a step-scorer to each document in a ranking.
            augment :: Ranking Score ((a, FeatureVec f s Double), relevance)
                    -> Ranking Score (Step s -> Score, ((a, FeatureVec f s Double), relevance))
            augment = mapEvalRanking cutoff updateScorer
              where
                -- Recompute the scoreFun and the document's current score.
                updateScorer :: Score
                             -> ((a, FeatureVec f s Double), relevance)
                             -> (Score, (Step s -> Score, ((a, FeatureVec f s Double), relevance)))
                updateScorer _ things@((_, fVec), _) = (stepScorer zeroStep, (stepScorer, things))
                  where !stepScorer = scoreStepOracle w0 fVec

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
