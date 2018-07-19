{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.Ranking.Evaluation
    ( ScoringMetric
    , TotalRel
    , meanAvgPrec
    ) where

import Data.List
import Data.Maybe
import qualified Data.Map.Strict as M

import SimplIR.Ranking as Ranking

-- | A scoring method, taking a set of queries and their rankings to a score.
type ScoringMetric rel qid a = M.Map qid (Ranking Double (a,rel)) -> Double

-- | The total number of relevant documents for a query.
type TotalRel = Int

meanAvgPrec :: (Ord rel)
            => (qid -> TotalRel) -> rel -> ScoringMetric rel qid a
meanAvgPrec totalRel relThresh rankings =
    mean (mapMaybe (\(qid, ranking) -> avgPrec relThresh (totalRel qid) ranking) (M.toList rankings))

mean :: (RealFrac a) => [a] -> a
mean xs = sum xs / realToFrac (length xs)

avgPrec :: forall rel doc. (Ord rel)
        => rel       -- ^ threshold of relevance
        -> TotalRel  -- ^ total number of relevant documents
        -> Ranking Double (doc, rel)  -- ^ ranking
        -> Maybe Double
avgPrec relThresh totalRel ranking
  | totalRel == 0 = Nothing
  | otherwise =
    let (_, relAtR) = mapAccumL numRelevantAt 0 rels
        rels :: [rel]
        rels = map (snd . snd) (Ranking.toSortedList ranking)

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
