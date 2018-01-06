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

import Codec.Serialise
import Control.DeepSeq
import Data.List
import Data.Monoid
import qualified Data.Map as M
import Control.Monad

import SimplIR.Types
import SimplIR.Utils.Compact

newtype DocIndexPath meta = DocIndexPath { getDocIndexPath :: FilePath }

-- | TODO
newtype DocIndex meta = DocIndex (M.Map DocumentId meta)

write :: (Serialise meta) => FilePath -> M.Map DocumentId meta -> IO (DocIndexPath meta)
write outFile docs = do
    writeFileSerialise outFile $ M.toAscList docs
    return $ DocIndexPath outFile
{-# INLINEABLE write #-}

open :: (Serialise meta, NFData meta) => DocIndexPath meta -> IO (DocIndex meta)
open (DocIndexPath file) =
    DocIndex . inCompact <$> readFileDeserialise file

lookupDoc :: Serialise meta => DocumentId -> DocIndex meta -> Maybe meta
lookupDoc docId (DocIndex idx) = M.lookup docId idx
{-# INLINEABLE lookupDoc #-}

size :: DocIndex meta -> Int
size (DocIndex idx) = M.size idx

documents :: (Serialise meta) => DocIndex meta -> [(DocumentId, meta)]
documents (DocIndex idx) = M.toAscList idx
{-# INLINEABLE documents #-}

merge :: forall meta. Serialise meta
      => DocIndexPath meta -> [DocIndex meta] -> IO [DocIdDelta]
merge (DocIndexPath dest) idxs = do
    -- First merge the document ids
    let docIds0 :: [DocIdDelta]
        (_, docIds0) = mapAccumL (\docId0 idx -> (docId0 <> toDocIdDelta (size idx), docId0))
                                 (DocIdDelta 0) idxs

    -- then write the document index
    void $ write dest $ M.unions
        [ M.mapKeys (`applyDocIdDelta` delta) idx
        | (delta, DocIndex idx) <- zip docIds0 idxs
        ]
    return docIds0
{-# INLINEABLE merge #-}
