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
    , naiveCoordAscent
    , coordAscent
    , miniBatched
    , miniBatchedAndEvaluated
    , defaultMiniBatchParams, MiniBatchParams(..)
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
import Linear.Epsilon

import SimplIR.FeatureSpace as FS
import SimplIR.Ranking as Ranking
import SimplIR.Ranking.Evaluation
import SimplIR.TrainUtils
import Debug.Trace as Debug

type Score = Double

newtype WeightVec f = WeightVec { getWeightVec :: FeatureVec f Double }
                    deriving (Show, Generic, NFData)

-- | Returns 'Nothing' if vector is near zero magnitude.
l2NormalizeWeightVec :: WeightVec f -> Maybe (WeightVec f)
l2NormalizeWeightVec (WeightVec w)
  | nearZero norm = Nothing
  | otherwise     = Just $ WeightVec $ recip norm `FS.scaleFeatureVec` w
  where norm = FS.l2Norm w

-- | Binary relevance judgement
data IsRelevant = NotRelevant | Relevant
                deriving (Ord, Eq, Show, Generic)
instance Hashable IsRelevant
instance NFData IsRelevant

stepFeature :: Step f -> WeightVec f -> WeightVec f
stepFeature (Step dim delta) (WeightVec v) =
    let x = v `FS.lookupIndex` dim
        !x' = x + delta
    in WeightVec $ v `FS.modifyIndices` [(dim, x')]

-- | All necessary data to produce a ranking of documents (along with relevance annotations), given the weight parameter
type FRanking f relevance a = [(a, FeatureVec f Double, relevance)]


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

data MiniBatchParams = MiniBatchParams { miniBatchParamsBatchSteps :: Int   -- ^ iterations per mini-batch
                                       , miniBatchParamsBatchSize :: Int    -- ^ mini-batch size
                                       , miniBatchParamsEvalSteps :: Int    -- ^ mini-batches per evaluation cycle
                                       }
                                       deriving Show
defaultMiniBatchParams :: MiniBatchParams
defaultMiniBatchParams = MiniBatchParams 4 100 10

miniBatchedAndEvaluated
    :: forall a f qid relevance gen.
       (Random.RandomGen gen, Show qid, Ord qid, Show a, Show relevance, Show f)
    => MiniBatchParams
    -> ScoringMetric relevance qid a
       -- ^ evaluation metric
    -> (gen -> WeightVec f -> M.Map qid (FRanking f relevance a) -> [WeightVec f])
       -- ^ optimiser (e.g. 'coordAscent')
    -> gen
    -> WeightVec f  -- ^ initial weights
    -> M.Map qid (FRanking f relevance a)
    -> [(Score, WeightVec f)]
       -- ^ list of evaluation iterates with evaluation metric
miniBatchedAndEvaluated (MiniBatchParams batchSteps batchSize evalSteps) evalMetric
                        optimise gen00 w00 fRankings =
    go $ miniBatched batchSteps batchSize optimise gen00 w00 fRankings
  where
    -- shuffle tuples for 'rerank'
    fRankings' = fmap (map (\(doc,feats,rel) -> ((doc,rel),feats))) fRankings

    go :: [WeightVec f] -> [(Score, WeightVec f)]
    go iters =
        let w:rest = drop evalSteps iters

            rankings :: M.Map qid (Ranking Score (a, relevance))
            rankings = fmap (rerank w) fRankings'
        in (evalMetric rankings, w) : go rest


naiveCoordAscent
    :: forall a f qid d gen relevance.
       (Random.RandomGen gen, Show qid, Show a, Show f)
    => ScoringMetric relevance qid a
    -> (d -> WeightVec f -> Ranking Double (a,relevance))
       -- ^ re-ranking function
    -> gen
    -> WeightVec f             -- ^ initial weights
    -> M.Map qid d             -- ^ training data
    -> [(Score, WeightVec f)]  -- ^ list of iterates
naiveCoordAscent scoreRanking rerank gen0 w0 fRankings =
    let score0 = scoreRanking $ fmap (\d -> rerank d w0) fRankings
    in go gen0 (score0, w0)
  where
    fspace = featureSpace $ getWeightVec w0
    dim = FS.featureDimension fspace

    deltas = [ f x
             | x <- [0.0001 * 2^n | n <- [1..25::Int]]
             , f <- [id, negate]
             ] ++ [0]

    go :: gen -> (Score, WeightVec f) -> [(Score, WeightVec f)]
    go gen (s,w) = (s',w') : go gen' (s',w')
      where
        !(s', w') = foldl' updateDim (s,w) dims
        dims = shuffle' (featureIndexes fspace) dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, WeightVec f) -> FeatureIndex f -> (Score, WeightVec f)
    updateDim (score0, w0) dim
      | null steps = Debug.trace ("coord dim "<> show dim<> " show score0 "<> show score0 <> ": No valid steps in ") $ (score0, w0)
      | otherwise  = trShow ("coord dim "<> show dim<> " show score0 "<> show score0 <> ": Found max: ")  $ maximumBy (comparing fst) steps
      where
        steps :: [(Score, WeightVec f)]
        steps =
            [ (score, w')
            | delta <- deltas
            , let step = Step dim delta
            , Just w' <- pure $ l2NormalizeWeightVec $ stepFeature step w0
            , let score = scoreRanking $ fmap (\d -> rerank d w') fRankings
            , not $ isNaN score
            ]
    trShow y x = Debug.trace (show y <> " " <> show x) x

coordAscent :: forall a f qid relevance gen.
               (Random.RandomGen gen, Show qid, Show a, Show f)
            => ScoringMetric relevance qid a
            -> gen
            -> WeightVec f   -- ^ initial weights
            -> M.Map qid (FRanking f relevance a)
            -> [(Score, WeightVec f)] -- ^ list of iterates
coordAscent scoreRanking gen0 w0 fRankings
  | isNaN score0         = error "coordAscent: Initial score is not a number"
  | Just msg <- badMsg   = error msg
  | otherwise            = go gen0 (score0, w0)
  where
    score0 = scoreRanking $ fmap (rerank w0 . map (\(doc, feats, rel) -> ((doc, rel), feats))) fRankings

    fspace = featureSpace $ getWeightVec w0
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
        !w' = foldl' updateDim w dims
        dims = shuffle' (featureIndexes fspace) dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, WeightVec f) -> FeatureIndex f -> (Score, WeightVec f)
    updateDim (score0, w0) dim
      | null steps = (score0, w0)
      | otherwise  = maximumBy (comparing fst) steps
      where
        steps :: [(Score, WeightVec f)]
        steps =
            [ (scoreStep step, w')  -- possibly l2Normalize  requires to invalidate scoredRaking caches and a full recomputation of the scores
            | delta <- deltas
            , let step = Step dim delta
            , Just w' <- pure $ l2NormalizeWeightVec $ stepFeature step w0
            ]
        scoreStep :: Step f -> Score
        scoreStep step
          | isNaN s   = error "coordAscent: Score is NaN"
          | otherwise = s
          where
            s = scoreRanking $ fmap (Ranking.fromList . map newScorer') cachedScoredRankings
            newScorer' ::  (a, Step f -> Score, relevance) -> (Score, (a, relevance))
            newScorer' (doc,scorer,rel) = (scorer step, (doc, rel))

        cachedScoredRankings :: M.Map qid [(a, Step f -> Score, relevance)]
        cachedScoredRankings =
            fmap newScorer fRankings
          where
            newScorer :: FRanking f relevance a -> [(a, Step f -> Score, relevance)]
            newScorer = docSorted . map (middle (\f -> scoreStepOracle w0 f))

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
