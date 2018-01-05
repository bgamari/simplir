{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveGeneric #-}

module SimplIR.DiskIndex.Posting2.Internal
    ( PostingIndexPath(..)
    , PostingIndex
    , walkTermPostings
    , open
    , fromTermPostings
    , decodeTermPostings
    , toPostingsLists
      -- * Internal
    , fromChunks
    , postingIndex
    , TermPostings(..)
    ) where

import GHC.Generics
import Data.Semigroup
import Codec.Serialise
import System.FilePath

import qualified Data.Map as M

import SimplIR.Types
import SimplIR.DiskIndex.Posting2.PostingList
import qualified SimplIR.DiskIndex.Posting2.CborList as CL
import qualified SimplIR.EncodedList.Cbor as ELC

newtype PostingIndexPath term p = PostingIndexPath { getPostingIndexPath :: FilePath }
                                deriving (Show)

postingListPath :: PostingIndexPath term p -> FilePath
postingListPath (PostingIndexPath x) = x <.> ".postings"

metadataPath :: PostingIndexPath term p -> FilePath
metadataPath (PostingIndexPath x) = x <.> ".meta"

data Metadata = Metadata
              deriving (Generic, Show)
instance Serialise Metadata

data PostingIndex term p = PostingIndex { _metadata :: !Metadata
                                        , postingIndex :: !(CL.CborList (TermPostings term p))
                                        }

data TermPostings term p = TermPostings term (ELC.EncodedList (PostingsChunk p))
                         deriving (Show)

instance (Serialise term, Serialise p) => Serialise (TermPostings term p) where
    decode = TermPostings <$> decode <*> decode
    encode (TermPostings a b) = encode a <> encode b

decodeTermPostings :: Serialise p => TermPostings term p -> (term, [Posting p])
decodeTermPostings (TermPostings t ps) =
    (t, foldMap decodeChunk $ ELC.toList ps)


-- | Build an inverted index from a set of postings.
fromTermPostings :: forall term p. (Serialise term, Serialise p)
                 => Int                       -- ^ how many postings per postings chunk
                 -> FilePath                  -- ^ output path
                 -> M.Map term [Posting p]    -- ^ postings for each term,
                                              -- must be sorted by document
                 -> IO (PostingIndexPath term p)
fromTermPostings chunkSize path termPostings = do
    let chunks :: [TermPostings term p]
        chunks =
            [ TermPostings term $ ELC.fromList $ chunkPostings chunkSize ps
            | (term, ps) <- M.toAscList termPostings
            ]
    fromChunks path chunks
{-# INLINEABLE fromTermPostings #-}

fromChunks :: forall term p. (Serialise term, Serialise p)
           => FilePath                  -- ^ output path
           -> [TermPostings term p]
           -> IO (PostingIndexPath term p)
fromChunks path chunks = do
    let out = PostingIndexPath path
    _ <- CL.fromList (postingListPath out) chunks
    writeFileSerialise (metadataPath out) Metadata
    return out
{-# INLINEABLE fromChunks #-}

walkTermPostings :: (Serialise term, Serialise p)
                 => PostingIndexPath term p
                 -> IO [TermPostings term p]
walkTermPostings = CL.walk . CL.CborListPath . postingListPath
{-# INLINEABLE walkTermPostings #-}

toPostingsLists :: (Serialise term, Serialise p)
                => PostingIndex term p
                -> [(term, [Posting p])]
toPostingsLists pidx =
    decodeTermPostings <$> CL.toList (postingIndex pidx)

open :: PostingIndexPath term p
     -> IO (PostingIndex term p)
open path = do
    termPostings <- CL.open $ CL.CborListPath (postingListPath path)
    meta <- readFileDeserialise (metadataPath path)
    return $ PostingIndex meta termPostings

_test :: M.Map String [Posting Int]
_test = M.fromList
    [ ("hello", p [(0, 1), (1, 3), (3, 1)])
    , ("world", p [(1, 1), (2, 3), (3, 1)])
    ]
  where
    p = map (\(did,x) -> Posting (DocId did) x)
