{-# LANGUAGE DeriveGeneric #-}

module SimplIR.DiskIndex.Posting2.PostingList where

import GHC.Generics

import qualified Data.Vector as V
import Codec.Serialise

import SimplIR.Encoded.Cbor as E
import SimplIR.Types

--------------------------------------------------------------------------------
-- Posting list representation

data PostingsChunk p = Chunk { startDocId :: !DocumentId
                             , postings   :: (E.Encoded (V.Vector (DocIdDelta, p)))
                             }
                     deriving (Show, Generic)

instance Serialise p => Serialise (PostingsChunk p)

applyDocIdDeltaToChunk :: DocIdDelta -> PostingsChunk p -> PostingsChunk p
applyDocIdDeltaToChunk delta (Chunk did postings) =
    Chunk (did `applyDocIdDelta` delta) postings

decodeChunk :: Serialise p => PostingsChunk p -> [Posting p]
decodeChunk (Chunk firstDocId encPostings) =
    let unDelta :: (DocIdDelta, p) -> Posting p
        unDelta (delta, p) =
            Posting (firstDocId `applyDocIdDelta` delta) p
    in map unDelta $ V.toList $ E.decode encPostings
{-# INLINEABLE decodeChunk #-}

-- | Split a list of 'Posting's into 'PostingsChunk's.
chunkPostings :: Serialise p => Int -> [Posting p] -> [PostingsChunk p]
chunkPostings n = go
  where
    go [] = []
    go xs = let (ys, rest) = splitAt n xs
                firstDocId :: DocumentId
                firstDocId = postingDocId $ head ys

                toDelta :: Posting p -> (DocIdDelta, p)
                toDelta (Posting docId p) = (firstDocId `docIdDelta` docId, p)

                encodedPostings = E.encode $ V.fromListN n $ map toDelta ys

            in (Chunk firstDocId encodedPostings : go rest)
