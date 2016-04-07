{-# LANGUAGE OverloadedStrings #-}

module DiskPostings.Merge where

import Data.Bifunctor
import qualified Data.Heap as H
import qualified Data.Vector as V

import Data.Binary

import Pipes
import DiskPostings
import Types
import qualified Encoded as E
import qualified EncodedList as EL
import BTree (BLeaf(..))

mergePostings :: (Binary p)
              => FilePath     -- ^ Merged output
              -> Int          -- ^ Chunk size
              -> [(DocIdDelta, [(Term, [PostingsChunk p])])]
              -- ^ a set of posting sources, along with their 'DocumentId' offsets
              -> IO ()
mergePostings outFile chunkSize =
    writePostings outFile chunkSize
    . each
    . map (\(term, chunks) -> BLeaf term (EL.fromList chunks))
    . mergeTermPostings

data SrcTerm p = SrcTerm { srcTerm   :: !Term
                         , srcDelta  :: !DocIdDelta
                         , srcChunks :: [PostingsChunk p]
                         }

instance Ord (SrcTerm p) where
    a `compare` b = (srcTerm a, srcDelta a) `compare` (srcTerm b, srcDelta b)

instance Eq (SrcTerm p) where
    a == b = a `compare` b == EQ

mergeTermPostings :: [(DocIdDelta, [(Term, [PostingsChunk p])])]
                  -> [(Term, [PostingsChunk p])]
mergeTermPostings = \srcs ->
    go $ H.fromList [ SrcTerm term delta chunks
                    | (delta, terms) <- srcs
                    , (term, chunks) <- terms
                    ]
  where
    go :: H.Heap (SrcTerm p) -> [(Term, [PostingsChunk p])]
    go srcs
      | Just (term, postingLists, srcs') <- popTerm srcs =
        let postingLists' =
                [ chunk `advanceDelta` delta
                | (delta, chunks) <- postingLists
                , chunk <- chunks
                ]

            advanceDelta :: PostingsChunk p -> DocIdDelta -> PostingsChunk p
            advanceDelta (Chunk docId chunks) delta =
                Chunk (docId `applyDocIdDelta` delta) chunks

        in (term, postingLists') : go srcs'

      | otherwise   = []

popTerm :: H.Heap (SrcTerm p)
        -> Maybe (Term, [(DocIdDelta, [PostingsChunk p])], H.Heap (SrcTerm p))
popTerm srcs
  | H.null srcs = Nothing
  | otherwise   =
    let term = srcTerm $ H.minimum srcs
        (popped, rest) = H.span (\x -> srcTerm x == term) srcs
        postingLists = map (\x -> (srcDelta x, srcChunks x)) $ H.toUnsortedList popped
    in Just (term, postingLists, rest)

test :: [(DocIdDelta, [(Term, [PostingsChunk ()])])]
test =
    [ (DocIdDelta 0,
       [ ("turtle", [chunk [0,1,4], chunk [10, 15, 18]])
       , ("cat",    [chunk [0,4],   chunk [11, 15, 19]])
       ])
    , (DocIdDelta 100,
       [ ("turtle", [chunk [0,1,4], chunk [10, 15, 18]])
       , ("dog",    [chunk [0,4],   chunk [11, 18, 19]])
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
