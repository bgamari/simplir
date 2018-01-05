module SimplIR.DiskIndex.Posting2
    ( PostingIndexPath(..)
    , PostingIndex
    , walkTermPostings
    , open
    , fromTermPostings
    , decodeTermPostings
    , toPostingsLists
    ) where

import SimplIR.DiskIndex.Posting2.Internal
