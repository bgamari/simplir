{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

-- | Build disk indexes in a memory-friendly manner.
module SimplIR.DiskIndex.Build
    ( buildIndex
    ) where

import Control.Monad ((>=>))
import Control.Monad.IO.Class
import Data.Profunctor
import System.Directory (removeDirectory)

import qualified Data.Map.Strict as M
import qualified Control.Foldl as Foldl
import Data.Binary

import qualified SimplIR.DiskIndex as DiskIdx
import SimplIR.Term as Term
import SimplIR.Types
import SimplIR.Utils

buildIndex :: (MonadIO m, Binary docmeta, Binary p)
           => Int     -- ^ How many documents to include in an index chunk?
           -> FilePath  -- ^ Final index path
           -> Foldl.FoldM m (docmeta, M.Map Term p) (DiskIdx.OnDiskIndex docmeta p)
buildIndex chunkSize outputPath =
    postmapM' mergeChunks
    $ foldChunksOf chunkSize (Foldl.generalize collectIndex) mergeIndexes
  where
    mergeChunks chunks = liftIO $ do
        idxs <- mapM DiskIdx.openOnDiskIndex chunks
        DiskIdx.merge outputPath idxs
        mapM_ (removeDirectory . DiskIdx.onDiskIndexPath) chunks
        return (DiskIdx.OnDiskIndex outputPath)

-- | Write and ultimately merge a set of index chunks.
mergeIndexes :: forall docmeta p m. (MonadIO m, Binary docmeta, Binary p)
             => Foldl.FoldM m ([(DocumentId, docmeta)], M.Map Term [Posting p])
                              [DiskIdx.OnDiskIndex docmeta p]
mergeIndexes =
    zipFoldM 0 succ
    $ premapM' chunkToIndex
    $ Foldl.generalize Foldl.list
  where
    chunkToIndex :: (Int, ([(DocumentId, docmeta)], M.Map Term [Posting p]))
                 -> m (DiskIdx.OnDiskIndex docmeta p)
    chunkToIndex (partN, (docIdx, postingIdx)) = liftIO $ do
        DiskIdx.fromDocuments path docIdx postingIdx
        return (DiskIdx.OnDiskIndex path)
      where
        path = "part-"++show partN++".index"

-- | Build an index chunk in memory.
collectIndex :: forall p docmeta.
              Foldl.Fold (docmeta, M.Map Term p)
                         ([(DocumentId, docmeta)], M.Map Term [Posting p])
collectIndex =
    zipFold (DocId 0) succ
    ((,) <$> docIdx <*> termIdx)
  where
    docIdx :: Foldl.Fold (DocumentId, (docmeta, M.Map Term p)) ([(DocumentId, docmeta)])
    docIdx =
        lmap (\(docId, (meta, _)) -> (docId, meta)) Foldl.list

    termIdx :: Foldl.Fold (DocumentId, (docmeta, M.Map Term p)) (M.Map Term [Posting p])
    termIdx =
        lmap (\(docId, (_, terms)) -> foldMap (toPosting docId) $ M.toList terms) Foldl.mconcat

    toPosting :: DocumentId -> (Term, p) -> M.Map Term [Posting p]
    toPosting docId (term, p) = M.singleton term $ [Posting docId p]

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

zipFold :: forall i a b.
           i -> (i -> i)
        -> Foldl.Fold (i, a) b
        -> Foldl.Fold a b
zipFold idx0 succ (Foldl.Fold step0 initial0 extract0) =
    Foldl.Fold step initial extract
  where
    initial = (idx0, initial0)
    extract = extract0 . snd
    step (!idx, s) x =
        let s' = step0 s (idx, x)
        in (succ idx, s')

premapM' :: Monad m
         => (a -> m b)
         -> Foldl.FoldM m b c
         -> Foldl.FoldM m a c
premapM' f (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step initial0 extract0
  where
    step s x = f x >>= step0 s

postmapM' :: Monad m
          => (b -> m c)
          -> Foldl.FoldM m a b
          -> Foldl.FoldM m a c
postmapM' f (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step0 initial0 (extract0 >=> f)
