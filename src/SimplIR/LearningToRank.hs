{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# LANGUAGE FlexibleContexts #-}

module SimplIR.LearningToRank
    ( -- * Basic types
      Score
    , Ranking
    , TotalRel
    , Rankings
    , Ranking(..)
      -- * Features
    , Features(..)
    , NormFeatures
    , zNormalizer
    , Normalization(..)
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
import Data.Ord
import Data.List
import Data.Maybe
import qualified System.Random as Random
import System.Random.Shuffle
import Data.Hashable
import qualified Data.Map as M
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as V

type Score = Double

-- | Binary relevance judgement
data IsRelevant = NotRelevant | Relevant
                deriving (Ord, Eq, Show, Generic)
instance Hashable IsRelevant

-- | A ranking of documents
newtype Ranking a = Ranking { getRanking :: [(Score, a)] }
                  deriving (Show)

-- | The total number of relevant documents for a query.
type TotalRel = Int

-- | A collection of rankings for a set of queries.
type Rankings rel qid a = M.Map qid (Ranking (a, rel))

type ScoringMetric rel qid a = Rankings rel qid a -> Double

meanAvgPrec :: (Ord rel)
            => (qid -> TotalRel) -> rel -> ScoringMetric rel qid a
meanAvgPrec totalRel relThresh rankings =
    mean (mapMaybe (\(qid, ranking) -> avgPrec relThresh (totalRel qid) ranking) (M.toList rankings))

mean :: (RealFrac a) => [a] -> a
mean xs = sum xs / realToFrac (length xs)

avgPrec :: forall rel doc. (Ord rel)
        => rel -> TotalRel -> Ranking (doc, rel) -> Maybe Double
avgPrec relThresh totalRel ranking
  | totalRel == 0 = Nothing
  | otherwise =
    let (_, relAtR) = mapAccumL numRelevantAt 0 rels
        rels :: [rel]
        rels = map (snd . snd) (getRanking ranking)

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


newtype Features = Features { getFeatures :: VU.Vector Double }
                 deriving (Show, Eq)
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

mkRanking :: [(Score, a)] -> Ranking a
mkRanking = Ranking . sortBy (flip $ comparing fst)

-- | Re-rank a set of documents given a weight vector.
rerank :: Features -> [(a, Features)] -> Ranking a
rerank weight fRanking =
    mkRanking
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

type NormFeatures = Features

data Normalization = Normalization { normFeatures   :: Features -> NormFeatures
                                   , denormFeatures :: NormFeatures -> Features
                                     -- | un-normalize feature weights (up to sort order)
                                   , denormWeights  :: Weight -> Weight
                                   }

zNormalizer :: [Features] -> Normalization
zNormalizer feats =
    Normalization
      { normFeatures   = \(Features xs) -> Features $ (xs ^-^ mean) ^/^ std'
      , denormFeatures = \(Features xs) -> Features $ (xs ^*^ std')  ^+^ mean
      , denormWeights  = \(Features xs) -> Features $ (xs ^/^ std')
      }
  where
    (mean, std) = featureMeanDev feats
    -- Ignore uniform features
    std' = VU.map f std
      where f 0 = 1
            f x = x

featureMeanDev :: [Features] -> (VU.Vector Double, VU.Vector Double)
featureMeanDev []    = error "featureMeanDev: no features"
featureMeanDev feats = (mean, std)
  where
    feats' = V.fromList $ map (\(Features xs) -> xs) feats
    mean = meanV feats'
    std  = VU.map sqrt $ meanV $ fmap (\xs -> VU.map squared $ xs ^-^ mean) feats'

    meanV :: V.Vector (VU.Vector Double) -> VU.Vector Double
    meanV xss = recip n *^ V.foldl1' (^+^) xss
      where n = realToFrac $ V.length xss

-- a few helpful utilities
squared :: Num a => a -> a
squared x = x*x

(*^) :: Double -> VU.Vector Double -> VU.Vector Double
s   *^ xs = VU.map (*s) xs

(^/^), (^*^), (^+^), (^-^) :: VU.Vector Double -> VU.Vector Double -> VU.Vector Double
xs ^/^ ys = VU.zipWith (/) xs ys
xs ^*^ ys = VU.zipWith (*) xs ys
xs ^+^ ys = VU.zipWith (+) xs ys
xs ^-^ ys = VU.zipWith (-) xs ys

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

coordAscent :: forall a qid relevance gen. (Random.RandomGen gen, Show Features, Show qid, Show a)
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
                                             , elem@(doc, fv, _) <- list
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
            scoreRanking $ fmap (mkRanking . map newScorer') cachedScoredRankings
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
