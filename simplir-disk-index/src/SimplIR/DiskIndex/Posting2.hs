module SimplIR.DiskIndex.Posting2
    ( PostingIndexPath(..)
    , PostingIndex
    , walkTermPostings
    , open
    , fromTermPostings
    , decodeTermPostings
    , toPostingsLists
    , termPostingsChunks'
      -- * Chunked
    , PostingsChunk
    , toPostingsChunks
    ) where

import SimplIR.DiskIndex.Posting2.Internal
import SimplIR.DiskIndex.Posting2.PostingList
