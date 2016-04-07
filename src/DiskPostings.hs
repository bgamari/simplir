{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables  #-}

module DiskPostings where

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

fromPostingsMap :: forall p. (Binary p)
                => Int                       -- ^ chunk size
                -> FilePath                  -- ^ file path
                -> M.Map Term [Posting p]    -- ^ postings
                -> IO ()
fromPostingsMap chunkSize path postings =
    let chunks :: [BTree.BLeaf Term (EL.EncodedList (PostingsChunk p))]
        chunks = map (toBLeaf . fmap (EL.fromList . chunkPostings chunkSize))
                 $ M.assocs postings
    in writePostings path (M.size postings) (each chunks)
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

newtype DiskPostings p = DiskPostings (BTree.LookupTree Term (EL.EncodedList (PostingsChunk p)))

openPostings :: FilePath -> IO (Either String (DiskPostings p))
openPostings path = liftIO $ fmap (fmap DiskPostings) (BTree.open path)

lookupPostings :: (Binary p)
               => DiskPostings p -> Term -> Maybe [Posting p]
lookupPostings (DiskPostings btree) term =
    foldMap decodeChunk . EL.toList <$> BTree.lookup btree term

walkPostings :: (Binary p)
             => DiskPostings p
             -> [(Term, [Posting p])]
walkPostings = map (fmap (foldMap decodeChunk)) . walkPostings'

walkPostings' :: (Binary p)
              => DiskPostings p
              -> [(Term, [PostingsChunk p])]
walkPostings' (DiskPostings btree) =
    map (fmap EL.toList . fromBLeaf) $ PP.toList $ void $ BTree.walkLeaves btree
  where
    fromBLeaf (BTree.BLeaf k v) = (k, v)

writePostings :: MonadIO m
              => FilePath -> Int
              -> Producer (BLeaf Term (EL.EncodedList (PostingsChunk p))) m ()
              -> m ()
writePostings path size =
    BTree.fromOrderedToFile 32 (fromIntegral size) path
