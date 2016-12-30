{-# LANGUAGE ScopedTypeVariables #-}
module LearningToRank where

import Data.Ord
import Data.List
import Data.Bifunctor
import qualified Data.Map as M
import qualified Data.Vector.Unboxed as V

type Score = Double
data IsRelevant = NotRel | Relevant
                deriving (Ord, Eq, Show)

-- | A ranking of documents along with relevant annotations
type Ranking relevance a = [(a, Score, relevance)]

-- | The total number of relevant documents for a query.
type TotalRel = Int

-- | A collection of rankings for a set of queries.
type Rankings rel qid a = M.Map qid (Ranking rel a, TotalRel)

meanAvgPrec :: (Ord rel)
            => rel -> Rankings rel qid a -> Double
meanAvgPrec relThresh rankings =
    mean (fmap (avgPrec relThresh) (M.elems rankings))

mean :: (RealFrac a) => [a] -> a
mean xs = sum xs / realToFrac (length xs)

avgPrec :: forall rel a. (Ord rel)
        => rel -> (Ranking rel a, TotalRel) -> Double
avgPrec relThresh (ranking, totalRel) =
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
    in
        sum precAtRelevantRanks / realToFrac totalRel


newtype Features = Features (V.Vector Double)
                 deriving (Show)

featureDim :: Features -> Int
featureDim (Features v) = V.length v

step :: Int -> Double -> Features -> Features
step dim delta (Features v) = Features $ V.update v (V.fromList [(dim, v V.! dim + delta)])

-- | A ranking of documents along with relevant annotations
type FRanking relevance a = [(a, Features, relevance)]

supervisedRanking :: Features -> FRanking relevance a -> Ranking relevance a
supervisedRanking weights fRanking =
    sortBy (comparing (\(_,score,_) -> score))
    $ map (\(doc, feat, rel) -> (doc, feat `dot` weights, rel)) fRanking

dot :: Features -> Features -> Double
dot (Features v) (Features u) = V.sum $ V.zipWith (*) v u

coordAscent :: Ord relevance
            => relevance
            -> Features -- ^ initial weights
            -> M.Map qid (FRanking relevance a, TotalRel)
            -> [(Score, Features)]
coordAscent relThresh w0 fRankings = iterate go (scoreWeights w0, w0)
  where
    scoreWeights :: Features -> Double
    scoreWeights w =
      meanAvgPrec relThresh $ fmap (first $ supervisedRanking w) fRankings

    dim = featureDim w0
    steps = [ f x
            | x <- [10, 1, 0.1, 0.01, 0.001]
            , f <- [id, negate]
            ]
    go :: (Score, Features) -> (Score, Features)
    go w =
      foldl' updateDim w [0..dim-1]

    updateDim :: (Score, Features) -> Int -> (Score, Features)
    updateDim (_, w) dim =
      maximumBy (comparing fst)
      [ (scoreWeights w', w')
      | delta <- steps
      , let w' = step dim delta w
      ]

-----------------------
-- Testing

type DocId = String
type TestRanking = Ranking IsRelevant DocId

rankingA :: TestRanking
rankingA =
    [ ("ben", 10, Relevant)
    , ("laura", 9, Relevant)
    , ("T!", 3, Relevant)
    , ("snowman", 4, NotRel)
    , ("cat", 5, NotRel)
    ]

rankingB :: TestRanking
rankingB =
    [ ("ben", 3, NotRel)
    , ("laura", 9, Relevant)
    , ("snowman", 4, Relevant)
    ]

rankingC :: TestRanking
rankingC =
    [ ("cat", 9, Relevant)
    , ("T!", 10, Relevant)
    ]

testFeatures :: M.Map Char (FRanking IsRelevant DocId, Int)
testFeatures =
    fmap (first $ map toFeatures) testRankings
  where
    toFeatures (a,b,c) = (a, Features $ V.fromList [1,b], c)

testRankings :: M.Map Char (TestRanking, Int)
testRankings =
    M.fromList $ zip ['a'..] $ zip [rankingA, rankingB, rankingC] (repeat 10)