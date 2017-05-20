{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ViewPatterns #-}

module SimplIR.DiskIndex.Posting.Merge where

import Data.Foldable
import Data.Monoid
import Data.Ord
import Data.List (sort, sortBy)
import qualified Data.Heap as H
import qualified Data.Vector as V
import Test.QuickCheck

import Data.Binary

import Pipes
import SimplIR.DiskIndex.Posting.Internal as PostingIdx
import SimplIR.DiskIndex.Posting.Types
import SimplIR.Types
import SimplIR.Term
import qualified SimplIR.Encoded as E
import qualified SimplIR.EncodedList as EL
import BTree (BLeaf(..))

merge :: forall p. (Binary p)
      => Int                -- ^ Chunk size
      -> PostingIndexPath p -- ^ Merged output
      -> Int                -- ^ Merged index size (in terms)
      -> [(DocIdDelta, [(Term, [PostingsChunk p])])]
      -- ^ a set of posting sources, along with their 'DocumentId' offsets
      -> IO ()
merge chunkSize outFile mergedSize =
    PostingIdx.write outFile mergedSize
    . each
    . map (\(term, chunks) -> BLeaf term (EL.fromList chunks))
    . map (fmap $ mergeChunks chunkSize)
    . mergePostings

mergePostings :: forall p. ()
              => [(DocIdDelta, [(Term, [PostingsChunk p])])]
              -> [(Term, [PostingsChunk p])]
mergePostings =
      interleavePostings . map applyDocIdDelta
  where
    applyDocIdDelta :: (DocIdDelta, [(Term, [PostingsChunk p])])
                    -> [(Term, [PostingsChunk p])]
    applyDocIdDelta (delta, terms) =
        map (fmap $ map $ applyDocIdDeltaToChunk delta) terms

-- | Merge small chunks
mergeChunks :: Int -> [PostingsChunk p] -> [PostingsChunk p]
mergeChunks _chunkSize = id -- TODO

-- | Given a set of $n$ sorted @[Entry p a]@s, lazily interleave them in sorted
-- order.
heapMerge :: forall p a. Ord p
          => [[H.Entry p a]] -> [H.Entry p a]
heapMerge = go . foldMap takeFirst
  where
    takeFirst :: [H.Entry p a] -> H.Heap (H.Entry p (a, [H.Entry p a]))
    takeFirst (H.Entry p x : rest) = H.singleton $ H.Entry p (x, rest)
    takeFirst []                   = H.empty

    go :: H.Heap (H.Entry p (a, [H.Entry p a]))
       -> [H.Entry p a]
    go xs
      | Just (H.Entry p (x, rest), xs') <- H.viewMin xs
      = H.Entry p x : go (xs' <> takeFirst rest)

      | otherwise = []

prop_heapMerge_length :: (Ord p) => [OrderedList (H.Entry p a)] -> Property
prop_heapMerge_length (map getOrdered -> ents) =
    property $ sum (map length ents) == length (heapMerge ents)

prop_heapMerge_sorted :: (Ord p, Eq a, Show p, Show a) => [OrderedList (H.Entry p a)] -> Property
prop_heapMerge_sorted (map getOrdered -> ents) =
    let merged = heapMerge ents
    in counterexample (show merged) (sort merged == merged)

instance (Arbitrary p, Arbitrary a) => Arbitrary (H.Entry p a) where
    arbitrary = H.Entry <$> arbitrary <*> arbitrary

interleavePostings :: [[(Term, [PostingsChunk p])]]
                   -> [(Term, [PostingsChunk p])]
interleavePostings = mergeTerms . heapMerge . map (map (uncurry H.Entry))
  where
    -- Merge chunks belonging to the same term
    mergeTerms :: [H.Entry Term [PostingsChunk p]] -> [(Term, [PostingsChunk p])]
    mergeTerms [] = []
    mergeTerms xs@(H.Entry term _chunks : _) =
        let (postingsOfTerm, rest) = span (\(H.Entry term' _) -> term == term') xs
        in ( term
           , fold $ sortBy (comparing $ startDocId . head) $ map H.payload postingsOfTerm
           ) : mergeTerms rest

test :: [(DocIdDelta, [(Term, [PostingsChunk ()])])]
test =
    [ (DocIdDelta 0,
       [ ("cat",    [chunk [0,4],   chunk [11, 15, 19]])
       , ("turtle", [chunk [0,1,4], chunk [10, 15, 18]])
       ])
    , (DocIdDelta 100,
       [ ("dog",    [chunk [0,4],   chunk [11, 18, 19]])
       , ("turtle", [chunk [0,1,4], chunk [10, 15, 18]])
       ])
    ]
  where
    chunk :: [Int] -> PostingsChunk ()
    chunk docIds' =
        Chunk docId0
              (E.encode $ V.fromList $ map (\x->(x `docIdDelta` docId0, ())) docIds)
      where
        docId0 = head docIds
        docIds = map DocId docIds'
