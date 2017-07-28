{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

-- | Build disk indexes in a memory-friendly manner.
module SimplIR.DiskIndex.Build
    ( buildIndex
    ) where

import Control.Monad.IO.Class
import Data.Profunctor
import System.Directory (removeDirectoryRecursive)
import System.IO.Temp

import qualified Data.Map.Strict as M
import qualified Control.Foldl as Foldl
import Data.Binary
import Pipes.Safe

import qualified SimplIR.DiskIndex as DiskIdx
import SimplIR.Types
import SimplIR.Utils

buildIndex :: forall term m doc p. (MonadSafe m, Binary term, Ord term, Binary doc, Binary p)
           => Int       -- ^ How many documents to include in an index chunk?
           -> FilePath  -- ^ Final index path
           -> Foldl.FoldM m (doc, M.Map term p) (DiskIdx.OnDiskIndex term doc p)
buildIndex chunkSize outputPath =
    postmapM' (mergeChunks outputPath)
    $ foldChunksOf chunkSize (Foldl.generalize collectIndex) mergeIndexes
{-# INLINEABLE buildIndex #-}

-- | Perform the final merge of a set of index chunks.
mergeChunks :: (MonadSafe m, Binary term, Ord term, Binary doc, Binary p)
            => FilePath
            -> [(DiskIdx.OnDiskIndex term doc p, ReleaseKey)]
            -> m (DiskIdx.OnDiskIndex term doc p)
mergeChunks outputPath chunks = do
    let (chunkFiles, chunkKeys) = unzip chunks
    idxs <- liftIO $ mapM DiskIdx.openOnDiskIndex chunkFiles
    liftIO $ DiskIdx.merge outputPath idxs
    mapM_ release chunkKeys
    return (DiskIdx.OnDiskIndex outputPath)
{-# INLINEABLE mergeChunks #-}

-- | Write a set of index chunks.
mergeIndexes :: forall term doc p m. (MonadSafe m, Binary term, Ord term, Binary doc, Binary p)
             => Foldl.FoldM m ([(DocumentId, doc)], M.Map term [Posting p])
                              [(DiskIdx.OnDiskIndex term doc p, ReleaseKey)]
mergeIndexes =
    premapM' chunkToIndex
    $ Foldl.generalize Foldl.list
  where
    chunkToIndex :: ([(DocumentId, doc)], M.Map term [Posting p])
                 -> m (DiskIdx.OnDiskIndex term doc p, ReleaseKey)
    chunkToIndex (docIdx, postingIdx) = do
        path <- liftIO $ createTempDirectory "." "part.index"
        key <- register $ liftIO $ removeDirectoryRecursive path
        liftIO $ DiskIdx.fromDocuments path docIdx postingIdx
        return (DiskIdx.OnDiskIndex path, key)
{-# INLINEABLE mergeIndexes #-}

-- | Build an index chunk in memory.
collectIndex :: forall term p doc. (Ord term)
             => Foldl.Fold (doc, M.Map term p)
                           ([(DocumentId, doc)], M.Map term [Posting p])
collectIndex =
    zipFold (DocId 0) succ
    ((,) <$> docIdx <*> termIdx)
  where
    docIdx :: Foldl.Fold (DocumentId, (doc, M.Map term p)) ([(DocumentId, doc)])
    docIdx =
        lmap (\(docId, (meta, _)) -> (docId, meta)) Foldl.list

    termIdx :: Foldl.Fold (DocumentId, (doc, M.Map term p)) (M.Map term [Posting p])
    termIdx =
        lmap (\(docId, (_, terms)) -> foldMap (toPosting docId) $ M.toList terms) Foldl.mconcat

    toPosting :: DocumentId -> (term, p) -> M.Map term [Posting p]
    toPosting docId (term, p) = M.singleton term $ [Posting docId p]
{-# INLINEABLE collectIndex #-}

