{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Document metadata index
module SimplIR.DiskIndex.Document
    ( DocIndex
    , open
    , write
      -- * Queries
    , lookupDoc
    , size
    , documents
      -- * On-disk
    , DocIndexPath(..)
    , merge
    ) where

import Control.Monad
import Data.Binary
import Data.List
import Data.Monoid
import qualified Data.Map as M
import System.Directory
import System.FilePath
import qualified BTree
import qualified Pipes as P
import qualified Pipes.Prelude as PP

import SimplIR.Types

-- | TODO
newtype DocIndex meta = DocIndex (BTree.LookupTree DocumentId meta)

treeOrder :: BTree.Order
treeOrder = 64

write :: Binary meta => DocIndexPath meta -> M.Map DocumentId meta -> IO ()
write (DocIndexPath outFile) docs = do
    createDirectoryIfMissing True (takeDirectory outFile)
    BTree.fromOrderedToFile treeOrder (fromIntegral $ M.size docs) outFile
        (P.each $ map (uncurry BTree.BLeaf) $ M.assocs docs)
{-# INLINEABLE write #-}

open :: DocIndexPath meta -> IO (DocIndex meta)
open (DocIndexPath file) =
    either uhOh (pure . DocIndex) =<< BTree.open file
  where
    uhOh e = fail $ "Failed to open document index "++show file++": "++e

lookupDoc :: Binary meta => DocumentId -> DocIndex meta -> Maybe meta
lookupDoc docId (DocIndex idx) = BTree.lookup idx docId
{-# INLINEABLE lookupDoc #-}

size :: DocIndex meta -> Int
size (DocIndex idx) = fromIntegral $ BTree.size idx

documents :: (Monad m, Binary meta) => DocIndex meta -> P.Producer (DocumentId, meta) m ()
documents (DocIndex idx) =
    void (BTree.walkLeaves idx) P.>-> PP.map toTuple
  where
    toTuple (BTree.BLeaf a b) = (a,b)
{-# INLINEABLE documents #-}

newtype DocIndexPath meta = DocIndexPath FilePath

merge :: forall meta. Binary meta
      => DocIndexPath meta -> [DocIndex meta] -> IO [DocIdDelta]
merge (DocIndexPath dest) idxs = do
    -- First merge the document ids
    let docIds0 :: [DocIdDelta]
        (_, docIds0) = mapAccumL (\docId0 idx -> (docId0 <> toDocIdDelta (size idx), docId0))
                                 (DocIdDelta 0) idxs
    let total = fromIntegral $ sum $ map size idxs

    -- then write the document index
    let toBLeaf delta (docId, meta) =
            BTree.BLeaf (docId `applyDocIdDelta` delta) meta
    BTree.fromOrderedToFile treeOrder total dest $ sequence
        [ documents idx P.>-> PP.map (toBLeaf delta)
        | (delta, idx) <- zip docIds0 idxs
        ]
    return docIds0
{-# INLINEABLE merge #-}
