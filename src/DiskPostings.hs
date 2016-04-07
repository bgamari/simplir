{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables  #-}

module DiskPostings where

import Data.Binary

import qualified Data.Map as M
import qualified Data.Vector as V
import Data.Vector.Binary () -- for instances

import Pipes
import qualified Pipes.Prelude as PP
import qualified BTree
import BTree (BLeaf(..))
import qualified EncodedList as EL
import Types

fromPostingsMap :: forall p. (Binary p)
                => Int                       -- ^ chunk size
                -> FilePath                  -- ^ file path
                -> M.Map Term [Posting p]    -- ^ postings
                -> IO ()
fromPostingsMap chunkSize path postings =
    let chunks :: [BTree.BLeaf Term (EL.EncodedList (PostingsChunk p))]
        chunks = map (toBLeaf . fmap (EL.fromList . chunksOf chunkSize))
                 $ M.assocs postings
    in writePostings path (M.size postings) (each chunks)
  where
    toBLeaf (a,b) = BTree.BLeaf a b

chunksOf :: Int -> [a] -> [V.Vector a]
chunksOf n = go
  where
    go [] = []
    go xs = let (ys, rest) = splitAt n xs
            in V.fromListN n ys : go rest

--type Delta = SmallNat
--data PostingsChunk p = Chunk DocumentId (V.Vector (Delta, Posting p))
type PostingsChunk p = V.Vector (Posting p)

newtype DiskPostings p = DiskPostings (BTree.LookupTree Term (EL.EncodedList (PostingsChunk p)))

openPostings :: FilePath -> IO (Either String (DiskPostings p))
openPostings path = liftIO $ fmap (fmap DiskPostings) (BTree.open path)

lookupPostings :: (Binary p)
               => DiskPostings p -> Term -> Maybe [Posting p]
lookupPostings (DiskPostings btree) term =
    foldMap V.toList . EL.toList <$> BTree.lookup btree term

walkPostings :: (Binary p)
             => DiskPostings p
             -> [(Term, [PostingsChunk p])]
walkPostings (DiskPostings btree) =
    map (fmap EL.toList . fromBLeaf) $ PP.toList $ void $ BTree.walkLeaves btree
  where
    fromBLeaf (BTree.BLeaf k v) = (k, v)

writePostings :: MonadIO m
              => FilePath -> Int
              -> Producer (BLeaf Term (EL.EncodedList (PostingsChunk p))) m ()
              -> m ()
writePostings path size =
    BTree.fromOrderedToFile 32 (fromIntegral size) path
