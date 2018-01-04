{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}

module SimplIR.DiskIndex.Posting.Types where

import GHC.Generics

import Data.Binary
import qualified Data.Vector as V
import Data.Vector.Binary () -- for instances

import qualified SimplIR.Encoded as E
import SimplIR.Types

data PostingsChunk p = Chunk { startDocId     :: !DocumentId
                             , postingsChunks :: !(E.Encoded (V.Vector (DocIdDelta, p)))
                             }
                     deriving (Show, Generic)

instance Binary p => Binary (PostingsChunk p)

applyDocIdDeltaToChunk :: DocIdDelta -> PostingsChunk p -> PostingsChunk p
applyDocIdDeltaToChunk delta (Chunk did postings) =
    Chunk (did `applyDocIdDelta` delta) postings
