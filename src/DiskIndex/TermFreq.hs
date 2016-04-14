{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables  #-}

module DiskIndex.TermFreq
    ( DiskIndex
    , fromTermPostings
    , openIndex
    , lookup
    , writeIndex
    , walk
    ) where

import GHC.Generics
import Control.Exception (assert)
import Data.Foldable

import Data.Binary
import qualified Data.Heap as H

import qualified Data.Map as M
import qualified Data.Vector as V
import Data.Vector.Binary () -- for instances

import Pipes
import qualified Pipes.Prelude as PP
import qualified BTree
import BTree (BLeaf(..))
import qualified EncodedList as EL
import qualified Encoded as E
import Data.SmallNat
import Types

import Prelude hiding (lookup)

-- | Build an inverted index from a set of postings.
fromTermPostings :: forall p. (Binary p)
                 => Int                       -- ^ chunk size
                 -> FilePath                  -- ^ file path
                 -> M.Map Term [Posting p]    -- ^ postings, must be sorted by term
                 -> IO ()
fromTermPostings chunkSize path postings =
    let chunks :: [BTree.BLeaf Term (EL.EncodedList (PostingsChunk p))]
        chunks = map (toBLeaf . fmap (EL.fromList . chunkPostings chunkSize))
                 $ M.assocs postings
    in writeIndex path (M.size postings) (each chunks)
  where
    toBLeaf (a,b) = BTree.BLeaf a b

newtype DocIdDelta = DocIdDelta SmallNat
                   deriving (Show, Binary, Enum, Bounded, Ord, Eq)

instance Monoid DocIdDelta where
    mempty = DocIdDelta 0
    DocIdDelta a `mappend` DocIdDelta b = DocIdDelta (a + b)

docIdDelta :: DocumentId -> DocumentId -> DocIdDelta
docIdDelta (DocId a) (DocId b)
  | delta < 0  = error "negative DocIdDelta"
  | otherwise  = DocIdDelta $ fromIntegral delta
  where delta = a - b

applyDocIdDelta :: DocumentId -> DocIdDelta -> DocumentId
applyDocIdDelta (DocId n) (DocIdDelta d) = DocId (n + fromIntegral d)

chunkPostings :: Binary p => Int -> [Posting p] -> [PostingsChunk p]
chunkPostings n = go
  where
    go [] = []
    go xs = let (ys, rest) = splitAt n xs
                firstDocId :: DocumentId
                firstDocId = postingDocId $ head ys

                toDelta :: Posting p -> (DocIdDelta, p)
                toDelta (Posting docId p) = (docId `docIdDelta` firstDocId, p)

            in (Chunk firstDocId $ E.encode $ V.fromListN n $ map toDelta ys) : go rest

decodeChunk :: Binary p => PostingsChunk p -> [Posting p]
decodeChunk (Chunk firstDocId encPostings) =
    let unDelta :: (DocIdDelta, p) -> Posting p
        unDelta (delta, p) =
            Posting (firstDocId `applyDocIdDelta` delta) p
    in map unDelta $ V.toList $ E.decode encPostings

data PostingsChunk p = Chunk DocumentId (E.Encoded (V.Vector (DocIdDelta, p)))
                     deriving (Show, Generic)

instance Binary p => Binary (PostingsChunk p)

newtype DiskIndex p = DiskIndex (BTree.LookupTree Term (EL.EncodedList (PostingsChunk p)))

openIndex :: FilePath -> IO (Either String (DiskIndex p))
openIndex path = liftIO $ fmap (fmap DiskIndex) (BTree.open path)

lookup :: (Binary p)
               => DiskIndex p -> Term -> Maybe [Posting p]
lookup (DiskIndex btree) term =
    foldMap decodeChunk . EL.toList <$> BTree.lookup btree term

walk :: (Binary p)
     => DiskIndex p
     -> [(Term, [Posting p])]
walk = map (fmap (foldMap decodeChunk)) . walk'

walk' :: (Binary p)
      => DiskIndex p
      -> [(Term, [PostingsChunk p])]
walk' (DiskIndex btree) =
    map (fmap EL.toList . fromBLeaf) $ PP.toList $ void $ BTree.walkLeaves btree
  where
    fromBLeaf (BTree.BLeaf k v) = (k, v)

writeIndex :: MonadIO m
           => FilePath -> Int
           -> Producer (BLeaf Term (EL.EncodedList (PostingsChunk p))) m ()
           -> m ()
writeIndex path size =
    BTree.fromOrderedToFile 32 (fromIntegral size) path
