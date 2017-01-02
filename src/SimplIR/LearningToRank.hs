{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module SimplIR.LearningToRank
    ( -- * Basic types
      Score
    , Ranking
    , TotalRel
    , Rankings
    , Features(..)
      -- * Computing rankings
    , weightRanking
      -- * Scoring metrics
    , ScoringMetric
    , meanAvgPrec
      -- * Learning
    , FRanking
    , coordAscent
      -- * Helpers
    , IsRelevant(..)
    ) where

import Data.Ord
import Data.List
import Data.Maybe
import qualified System.Random as Random
import System.Random.Shuffle
import qualified Data.Map as M
import qualified Data.Vector.Unboxed as V

type Score = Double

-- | Binary relevance judgement
data IsRelevant = NotRelevant | Relevant
                deriving (Ord, Eq, Show)

-- | A ranking of documents along with relevant annotations
type Ranking relevance a = [(a, Score, relevance)]

-- | The total number of relevant documents for a query.
type TotalRel = Int

-- | A collection of rankings for a set of queries.
type Rankings rel qid a = M.Map qid (Ranking rel a)

type ScoringMetric rel qid a = Rankings rel qid a -> Double


meanAvgPrec :: (Ord rel)
            => (qid -> TotalRel) -> rel -> ScoringMetric rel qid a
meanAvgPrec totalRel relThresh rankings =
    mean (mapMaybe (\(qid, ranking) -> avgPrec relThresh (totalRel qid) ranking) (M.toList rankings))

mean :: (RealFrac a) => [a] -> a
mean xs = sum xs / realToFrac (length xs)

avgPrec :: forall rel a. (Ord rel)
        => rel -> TotalRel -> Ranking rel a -> Maybe Double
avgPrec relThresh totalRel ranking
  | totalRel == 0 = Nothing
  | otherwise =
    let (_, relAtR) = mapAccumL numRelevantAt 0 rels
        rels = map (\(_, _, rel) -> rel) ranking

        numRelevantAt :: Int -> rel -> (Int, Int)
        numRelevantAt accum rel
          | rel >= relThresh = (accum + 1, accum + 1)
          | otherwise        = (accum, accum)

        precAtR :: [(Double, rel)]
        precAtR = zip (zipWith (\n k -> realToFrac n / k) relAtR [1..]) rels

        precAtRelevantRanks = [ prec
                              | (prec, rel) <- precAtR
                              , rel >= relThresh
                              ]
    in Just $ sum precAtRelevantRanks / realToFrac totalRel


newtype Features = Features (V.Vector Double)
                 deriving (Show, Eq)

featureDim :: Features -> Int
featureDim (Features v) = V.length v

stepFeature :: Step -> Features -> Features
stepFeature (Step dim delta) (Features v) =
    Features $ V.update v (V.fromList [(dim, v V.! dim + delta)])

-- | A ranking of documents along with relevance annotations
type FRanking relevance a = [(a, Features, relevance)]

dot :: Features -> Features -> Double
dot (Features v) (Features u) = V.sum $ V.zipWith (*) v u

-- | Re-rank a set of documents given a weight vector.
weightRanking :: Features -> FRanking rel a -> Ranking rel a
weightRanking weight fRanking =
    sortBy (flip $ comparing $ \(_,s,_) -> s)
    [ (doc, weight `dot` feats, rel)
    | (doc, feats, rel) <- fRanking
    ]

data Step = Step Int Double

zeroStep :: Step
zeroStep = Step 0 0

isZeroStep :: Step -> Bool
isZeroStep (Step _ d) = d == 0

-- | @dotStepOracle w f step == (w + step) `dot` f@.
-- produces scorers that make use of the original dot product - only applying local modifications induced by the step
scoreStepOracle :: Features -> Features -> (Step -> Score)
scoreStepOracle w@(Features w') f@(Features f') = scoreFun
  where
    scoreFun :: Step -> Score
    scoreFun s | isZeroStep s = score0
    scoreFun (Step dim delta) = score0 - scoreTerm dim 0 + scoreTerm dim delta

    -- scoreDeltaTerm: computes term contribution to dot product of w' + step
    scoreTerm dim off = (off + w' V.! dim) * (f' V.! dim)
    !score0 = w `dot` f

coordAscent :: forall a qid relevance gen. (Random.RandomGen gen)
            => gen
            -> ScoringMetric relevance qid a
            -> Features -- ^ initial weights
            -> M.Map qid (FRanking relevance a)
            -> [(Score, Features)]
coordAscent gen0 scoreRanking w0 fRankings = go gen0 (score0, w0)
  where
    score0 = scoreRanking $ fmap (weightRanking w0) fRankings
    dim = featureDim w0
    deltas = [ f x
             | x <- [0.001, 0.01, 0.1, 1, 10, 100, 1000]
             , f <- [id, negate]
             ] ++ [0]

    go :: gen -> (Score, Features) -> [(Score, Features)]
    go gen w = w' : go gen' w'
      where
        w' = foldl' updateDim w dims
        dims = shuffle' [0..dim-1] dim g
        (g, gen') = Random.split gen

    updateDim :: (Score, Features) -> Int -> (Score, Features)
    updateDim (_, w) dim =
        maximumBy (comparing fst)
        [ (scoreStep step, stepFeature step w)
        | delta <- deltas
        , let step = Step dim delta
        ]
      where
        scoreStep :: Step -> Score
        scoreStep step =
            scoreRanking $ fmap (docSorted . map newScorer') cachedScoredRankings
          where
            newScorer' ::  (a, Step -> Score, relevance) -> (a, Score, relevance)
            newScorer' = middle $ \scorer -> scorer step

            docSorted = sortBy (flip $ comparing $ \(_,score,_) -> score)

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

type DocId = String
type TestRanking = Ranking IsRelevant DocId

rankingA :: TestRanking
rankingA =
    [ ("ben", 10, Relevant)
    , ("laura", 9, Relevant)
    , ("T!", 3, Relevant)
    , ("snowman", 4, NotRelevant)
    , ("cat", 5, NotRelevant)
    ]

rankingB :: TestRanking
rankingB =
    [ ("ben", 3, NotRelevant)
    , ("laura", 9, Relevant)
    , ("snowman", 4, Relevant)
    ]

rankingC :: TestRanking
rankingC =
    [ ("cat", 9, Relevant)
    , ("T!", 10, Relevant)
    ]

testFeatures :: M.Map Char (FRanking IsRelevant DocId)
testFeatures =
    fmap (map toFeatures) testRankings
  where
    toFeatures (a,b,c) = (a, Features $ V.fromList [1,b], c)

testRankings :: M.Map Char TestRanking
testRankings =
    M.fromList $ zip ['a'..] [rankingA, rankingB, rankingC]
