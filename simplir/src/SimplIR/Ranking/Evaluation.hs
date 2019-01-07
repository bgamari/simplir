{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module SimplIR.Ranking.Evaluation
    ( ScoringMetric
    , TotalRel
    , meanAvgPrec
    , avgPrec
    ) where

import Data.List
import Data.Maybe
import qualified Data.Map.Strict as M

import SimplIR.Ranking (Ranking)
import qualified SimplIR.Ranking as Ranking

-- | A scoring method, taking a set of queries and their rankings to a score.
type ScoringMetric rel qid a = M.Map qid (Ranking Double (a,rel)) -> Double

-- | The total number of relevant documents for a query.
type TotalRel = Int

meanAvgPrec :: (Ord rel)
            => (qid -> TotalRel) -> rel -> ScoringMetric rel qid a
meanAvgPrec totalRel relThresh rankings
  | null xs = error "SimplIR.Ranking.Evaluation.meanAvgPrec: no queries with relevant documents"
  | otherwise = mean xs
  where xs = mapMaybe (\(qid, ranking) -> avgPrec relThresh (totalRel qid) ranking) (M.toList rankings)

mean :: (RealFrac a) => [a] -> a
mean xs = sum xs / realToFrac (length xs)

avgPrec :: forall rel doc score. (Ord rel)
        => rel       -- ^ threshold of relevance (inclusive)
        -> TotalRel  -- ^ total number of relevant documents
        -> Ranking score (doc, rel)  -- ^ ranking
        -> Maybe Double
avgPrec relThresh totalRel ranking
  | totalRel == 0 = Nothing
  | otherwise =
    let (_, relAtR) = mapAccumL numRelevantAt 0 rels
        rels :: [rel]
        rels = map (snd . snd) (Ranking.toSortedList ranking)

        numRelevantAt :: Int -> rel -> (Int, Int)
        numRelevantAt !accum rel
          | rel >= relThresh = let !accum' = accum + 1 in (accum', accum')
          | otherwise        = (accum, accum)

        precAtR :: [(Double, rel)]
        precAtR = zipWith3
                    (\n k rel -> let !prec = realToFrac n / realToFrac k
                                 in (prec, rel))
                    relAtR
                    [1 :: Int ..]
                    rels

        precAtRelevantRanks = [ prec
                              | (prec, rel) <- precAtR
                              , rel >= relThresh
                              ]
    in Just $! sum precAtRelevantRanks / realToFrac totalRel
