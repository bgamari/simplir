{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
 
module DiskIndex.Posting.Merge where

import Data.Bifunctor
import Data.Foldable
import Data.Monoid
import Data.Ord
import Data.List (sortBy)
import qualified Data.Heap as H
import qualified Data.Vector as V

import Data.Binary

import Pipes
import DiskIndex.Posting as PostingIdx
import DiskIndex.Posting.Types
import Types
import qualified Encoded as E
import qualified EncodedList as EL
import BTree (BLeaf(..))

merge :: forall p. (Binary p)
      => FilePath     -- ^ Merged output
      -> Int          -- ^ Chunk size
      -> [(DocIdDelta, [(Term, [PostingsChunk p])])]
      -- ^ a set of posting sources, along with their 'DocumentId' offsets
      -> IO ()
merge outFile chunkSize =
    PostingIdx.write outFile 64
    . each
    . map (\(term, chunks) -> BLeaf term (EL.fromList chunks))
    . map (fmap $ mergeChunks chunkSize)
    . mergePostings

mergePostings :: forall p. (Binary p)
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
mergeChunks chunkSize = id -- TODO

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

interleavePostings :: [[(Term, [PostingsChunk p])]]
                   -> [(Term, [PostingsChunk p])]
interleavePostings = mergeTerms . heapMerge . map (map (uncurry H.Entry))
  where
    -- Merge chunks belonging to the same term
    mergeTerms :: [H.Entry Term [PostingsChunk p]] -> [(Term, [PostingsChunk p])]
    mergeTerms [] = []
    mergeTerms xs@(H.Entry term chunks : _) =
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
    p :: Int -> Posting ()
    p n = Posting (DocId n) ()

    chunk :: [Int] -> PostingsChunk ()
    chunk docIds' =
        Chunk docId0
              (E.encode $ V.fromList $ map (\x->(x `docIdDelta` docId0, ())) docIds)
      where
        docId0 = head docIds
        docIds = map DocId docIds'
