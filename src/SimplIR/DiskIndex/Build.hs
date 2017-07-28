{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

-- | Build disk indexes in a memory-friendly manner.
module SimplIR.DiskIndex.Build
    ( buildIndex
    ) where

import Control.Monad ((>=>))
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

zipFoldM :: forall i m a b. Monad m
         => i -> (i -> i)
         -> Foldl.FoldM m (i, a) b
         -> Foldl.FoldM m a b
zipFoldM idx0 succ' (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step initial extract
  where
    initial = do s <- initial0
                 return (idx0, s)
    extract = extract0 . snd
    step (!idx, s) x = do
        s' <- step0 s (idx, x)
        return (succ' idx, s')
{-# INLINEABLE zipFoldM #-}

zipFold :: forall i a b.
           i -> (i -> i)
        -> Foldl.Fold (i, a) b
        -> Foldl.Fold a b
zipFold idx0 succ' (Foldl.Fold step0 initial0 extract0) =
    Foldl.Fold step initial extract
  where
    initial = (idx0, initial0)
    extract = extract0 . snd
    step (!idx, s) x =
        let s' = step0 s (idx, x)
        in (succ' idx, s')
{-# INLINEABLE zipFold #-}

premapM' :: Monad m
         => (a -> m b)
         -> Foldl.FoldM m b c
         -> Foldl.FoldM m a c
premapM' f (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step initial0 extract0
  where
    step s x = f x >>= step0 s
{-# INLINEABLE premapM' #-}

postmapM' :: Monad m
          => (b -> m c)
          -> Foldl.FoldM m a b
          -> Foldl.FoldM m a c
postmapM' f (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step0 initial0 (extract0 >=> f)
{-# INLINEABLE postmapM' #-}
