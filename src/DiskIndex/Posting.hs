{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DiskIndex.Posting
    ( module DiskIndex.Posting.Internal
      -- * On disk
    , PostingIndexPath(..)
    , merge
    ) where

import Data.Binary
import Data.Traversable
import DiskIndex.Posting.Internal
import DiskIndex.Posting.Types
import qualified DiskIndex.Posting.Merge as Merge
import Types

newtype PostingIndexPath p = PostingIndexPath FilePath

merge :: forall p. Binary p
      => PostingIndexPath p -> [(DocIdDelta, PostingIndexPath p)] -> IO ()
merge (PostingIndexPath destDir) parts = do
    idxs <- forM parts $ \(docIdMap, PostingIndexPath path) ->
        either (const $ error $ "Failed to open posting index: "++path) id <$> open path

    let allPostings :: [[(Term, [PostingsChunk p])]]
        allPostings = map walkChunks idxs
        docIds0 = map fst parts

    let mergedSize = sum $ map termCount idxs
    Merge.merge postingChunkSize destDir mergedSize (zip docIds0 allPostings)

postingChunkSize = 64
