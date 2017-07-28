{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables  #-}

module SimplIR.DiskIndex.Posting.Internal
    ( DiskIndex
    , PostingIndexPath(..)
    , fromTermPostings
    , open
    , lookup
    , write
    , walk
    , walkChunks
    , termCount
    ) where

import Data.Foldable

import Data.Binary

import qualified Data.Map as M
import qualified Data.Vector as V

import Pipes
import qualified Pipes.Prelude as PP

import qualified BTree
import BTree (BLeaf(..))

import qualified SimplIR.EncodedList as EL
import qualified SimplIR.Encoded as E
import SimplIR.DiskIndex.Posting.Types
import SimplIR.Types

import Prelude hiding (lookup)

newtype PostingIndexPath term p = PostingIndexPath { getPostingIndexPath :: FilePath }

-- | Build an inverted index from a set of postings.
fromTermPostings :: forall term p. (Binary term, Binary p)
                 => Int                       -- ^ how many postings per chunk
                 -> PostingIndexPath term p   -- ^ file path
                 -> M.Map term [Posting p]    -- ^ postings for each term,
                                              -- must be sorted by document
                 -> IO ()
fromTermPostings chunkSize path postings =
    let chunks :: [BTree.BLeaf term (EL.EncodedList (PostingsChunk p))]
        chunks = map (toBLeaf . fmap (EL.fromList . chunkPostings chunkSize))
                 $ M.assocs postings
    in write path (M.size postings) (each chunks)
  where
    toBLeaf (a,b) = BTree.BLeaf a b
{-# INLINEABLE fromTermPostings #-}

-- | Split a list of 'Posting's into 'PostingsChunk's.
chunkPostings :: Binary p => Int -> [Posting p] -> [PostingsChunk p]
chunkPostings n = go
  where
    go [] = []
    go xs = let (ys, rest) = splitAt n xs
                firstDocId :: DocumentId
                firstDocId = postingDocId $ head ys

                toDelta :: Posting p -> (DocIdDelta, p)
                toDelta (Posting docId p) = (firstDocId `docIdDelta` docId, p)

            in (Chunk firstDocId $ E.encode $ V.fromListN n $ map toDelta ys) : go rest

decodeChunk :: Binary p => PostingsChunk p -> [Posting p]
decodeChunk (Chunk firstDocId encPostings) =
    let unDelta :: (DocIdDelta, p) -> Posting p
        unDelta (delta, p) =
            Posting (firstDocId `applyDocIdDelta` delta) p
    in map unDelta $ V.toList $ E.decode encPostings
{-# INLINEABLE decodeChunk #-}


newtype DiskIndex term p = DiskIndex (BTree.LookupTree term (EL.EncodedList (PostingsChunk p)))

open :: PostingIndexPath term p -> IO (Either String (DiskIndex term p))
open (PostingIndexPath path) = liftIO $ fmap (fmap DiskIndex) (BTree.open path)

lookup :: (Binary term, Ord term, Binary p)
       => DiskIndex term p -> term -> Maybe [Posting p]
lookup (DiskIndex btree) term =
    foldMap decodeChunk . EL.toList <$> BTree.lookup btree term
{-# INLINEABLE lookup #-}

walk :: (Binary term, Binary p)
     => DiskIndex term p
     -> [(term, [Posting p])]
walk = map (fmap (foldMap decodeChunk)) . walkChunks
{-# INLINEABLE walk #-}

walkChunks :: (Binary term, Binary p)
           => DiskIndex term p
           -> [(term, [PostingsChunk p])]
walkChunks (DiskIndex btree) =
    map (fmap EL.toList . fromBLeaf) $ PP.toList $ void $ BTree.walkLeaves btree
  where
    fromBLeaf (BTree.BLeaf k v) = (k, v)
{-# INLINEABLE walkChunks #-}

write :: (Binary term, MonadIO m)
      => PostingIndexPath term p -> Int
      -> Producer (BLeaf term (EL.EncodedList (PostingsChunk p))) m ()
      -> m ()
write (PostingIndexPath path) size prod =
    BTree.fromOrderedToFile 32 (fromIntegral size) path prod
{-# INLINEABLE write #-}

-- | How many terms are in a 'DiskIndex'?
termCount :: DiskIndex term p -> Int
termCount (DiskIndex btree) = fromIntegral $ BTree.size btree
