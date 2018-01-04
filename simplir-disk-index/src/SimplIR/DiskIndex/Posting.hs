{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.DiskIndex.Posting
    ( module SimplIR.DiskIndex.Posting.Internal
      -- * On disk
    , PostingIndexPath(..)
    , merge
    ) where

import Data.Binary
import Data.Traversable

import           SimplIR.DiskIndex.Posting.Internal
import           SimplIR.DiskIndex.Posting.Types
import qualified SimplIR.DiskIndex.Posting.Merge as Merge
import           SimplIR.Types

merge :: forall term p. (Ord term, Binary term, Binary p)
      => PostingIndexPath term p -> [(DocIdDelta, PostingIndexPath term p)] -> IO ()
merge destDir parts = do
    idxs <- forM parts $ \(_docIdMap, path) ->
        either (const $ error $ "Failed to open posting index: "++getPostingIndexPath path) id
            <$> open path

    let allPostings :: [[(term, [PostingsChunk p])]]
        allPostings = map walkChunks idxs
        docIds0 = map fst parts

    let mergedSize = sum $ map termCount idxs
    Merge.merge postingChunkSize destDir mergedSize (zip docIds0 allPostings)
{-# INLINEABLE merge #-}

postingChunkSize :: Int
postingChunkSize = 64
