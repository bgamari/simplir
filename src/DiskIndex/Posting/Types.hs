{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}

module DiskIndex.Posting.Types where

import Data.Binary
import GHC.Generics
import Data.Monoid
import Data.SmallNat
import qualified Data.Vector as V
import Data.Vector.Binary () -- for instances
import qualified Encoded as E
import Types

newtype DocIdDelta = DocIdDelta SmallNat
                   deriving (Show, Binary, Enum, Bounded, Ord, Eq)

-- | Lift an 'Int' into a 'DocIdDelta'
toDocIdDelta :: Int -> DocIdDelta
toDocIdDelta n
  | n' >= minBound && n' < maxBound = DocIdDelta n'
  | otherwise                       = error "toDocIdDelta: Bad delta"
  where n' = fromIntegral n :: SmallNat

instance Monoid DocIdDelta where
    mempty = DocIdDelta 0
    DocIdDelta a `mappend` DocIdDelta b = DocIdDelta (a + b)

-- | Take the difference between two 'DocumentId's
docIdDelta :: DocumentId -> DocumentId -> DocIdDelta
docIdDelta (DocId a) (DocId b)
  | delta < 0  = error "negative DocIdDelta"
  | otherwise  = DocIdDelta $ fromIntegral delta
  where delta = a - b

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
