{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}

module DiskIndex.Posting.Types where

import GHC.Generics
import GHC.Stack
import Data.Monoid

import Data.Binary
import qualified Data.Vector as V
import Data.Vector.Binary () -- for instances

import Data.SmallNat
import qualified Encoded as E
import Types

newtype DocIdDelta = DocIdDelta SmallNat
                   deriving (Show, Binary, Enum, Bounded, Ord, Eq)

-- | Lift an 'Int' into a 'DocIdDelta'
toDocIdDelta :: HasCallStack => Int -> DocIdDelta
toDocIdDelta n
  | n' >= minBound && n' < maxBound = DocIdDelta n'
  | otherwise                       = error "toDocIdDelta: Bad delta"
  where n' = fromIntegral n :: SmallNat
{-# INLINE toDocIdDelta #-}

instance Monoid DocIdDelta where
    mempty = DocIdDelta 0
    DocIdDelta a `mappend` DocIdDelta b = DocIdDelta (a + b)

-- | Take the difference between two 'DocumentId's
docIdDelta :: HasCallStack => DocumentId -> DocumentId -> DocIdDelta
docIdDelta (DocId a) (DocId b)
  | delta < 0  = error "negative DocIdDelta"
  | otherwise  = DocIdDelta $ fromIntegral delta
  where delta = b - a
{-# INLINE docIdDelta #-}

applyDocIdDelta :: DocumentId -> DocIdDelta -> DocumentId
applyDocIdDelta (DocId n) (DocIdDelta d) = DocId (n + fromIntegral d)

applyDocIdDeltaToChunk :: DocIdDelta -> PostingsChunk p -> PostingsChunk p
applyDocIdDeltaToChunk delta (Chunk did postings) =
    Chunk (did `applyDocIdDelta` delta) postings

data PostingsChunk p = Chunk { startDocId     :: !DocumentId
                             , postingsChunks :: !(E.Encoded (V.Vector (DocIdDelta, p)))
                             }
                     deriving (Show, Generic)

instance Binary p => Binary (PostingsChunk p)
