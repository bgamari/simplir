{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}

module SimplIR.Ranking.Evaluation
    ( ScoringMetric
    , TotalRel
    , meanAvgPrec
    , avgPrec
    , naiveAvgPrec
    ) where

import Data.List
import Data.Maybe
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as VU

import SimplIR.Ranking (Ranking)
import SimplIR.Types.Relevance
import qualified SimplIR.Ranking as Ranking

-- | A scoring method, taking a set of queries and their rankings to a score.
type ScoringMetric rel qid = forall a. M.Map qid (Ranking Double (a,rel)) -> Double

-- | The total number of relevant documents for a query.
type TotalRel = Int

meanAvgPrec :: (Ord rel)
            => (qid -> TotalRel) -> rel -> ScoringMetric rel qid
meanAvgPrec totalRel relThresh rankings
  | null xs = error "SimplIR.Ranking.Evaluation.meanAvgPrec: no queries with relevant documents"
  | otherwise = mean xs
  where xs = mapMaybe (\(qid, ranking) -> avgPrec relThresh (totalRel qid) ranking) (M.toList rankings)

mean :: (RealFrac a) => [a] -> a
mean xs = sum xs / realToFrac (length xs)

naiveAvgPrec :: forall rel doc score. (Ord rel, VU.Unbox score)
             => rel       -- ^ threshold of relevance (inclusive)
             -> TotalRel  -- ^ total number of relevant documents
             -> Ranking score (doc, rel)  -- ^ ranking
             -> Maybe Double  -- ^ 'Nothing' if no relevant documents
naiveAvgPrec relThresh totalRel ranking
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

avgPrec :: forall rel doc score. (Ord rel, VU.Unbox score)
        => rel       -- ^ threshold of relevance (inclusive)
        -> TotalRel  -- ^ total number of relevant documents
        -> Ranking score (doc, rel)  -- ^ ranking
        -> Maybe Double  -- ^ 'Nothing' if no relevant documents
avgPrec relThresh totalRel ranking
  | totalRel == 0 = Nothing
  | otherwise =
    let precsAtR = zipWith (\(r, _) i -> realToFrac i / realToFrac r) relevantEnts [1 :: Int ..]
        relevantEnts = filter (\(_, (_, (_, rel))) -> rel >= relThresh)
                       $ zip [1 :: Int ..]
                       $ Ranking.toSortedList ranking
    in Just $ sum precsAtR / realToFrac totalRel
{-# SPECIALISE
    avgPrec :: forall rel doc. (Ord rel)
            => rel
            -> TotalRel
            -> Ranking Double (doc, rel)
            -> Maybe Double
    #-}
{-# SPECIALISE
    avgPrec :: IsRelevant
            -> TotalRel
            -> Ranking Double (doc, IsRelevant)
            -> Maybe Double
    #-}
