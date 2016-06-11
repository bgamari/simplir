{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Document metadata index
module DiskIndex.Document
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

import Data.Binary
import Data.Bifunctor
import Data.List
import Data.Monoid
import qualified Data.Map as M
import qualified Data.ByteString.Lazy as BSL
import System.Directory
import System.FilePath

import Types

-- | TODO
newtype DocIndex meta = DocIndex (M.Map DocumentId meta)
                      deriving (Binary)

write :: Binary meta => DocIndexPath meta -> M.Map DocumentId meta -> IO ()
write (DocIndexPath outFile) docs = do
    createDirectoryIfMissing True (takeDirectory outFile)
    BSL.writeFile outFile $ encode docs

open :: Binary meta => DocIndexPath meta -> IO (DocIndex meta)
open (DocIndexPath file) = decode <$> BSL.readFile file

lookupDoc :: DocumentId -> DocIndex meta -> Maybe meta
lookupDoc docId (DocIndex idx) = M.lookup docId idx

size :: DocIndex meta -> Int
size (DocIndex idx) = M.size idx

documents :: DocIndex meta -> [(DocumentId, meta)]
documents (DocIndex idx) = M.assocs idx


newtype DocIndexPath meta = DocIndexPath FilePath

merge :: forall meta. Binary meta
       => DocIndexPath meta -> [DocIndexPath meta] -> IO [DocIdDelta]
merge dest parts = do
    idxs <- mapM open parts
    -- First merge the document ids
    let docIds0 :: [DocIdDelta]
        (_, docIds0) = mapAccumL (\docId0 idx -> (docId0 <> toDocIdDelta (size idx), docId0))
                                 (DocIdDelta 0) idxs

    -- then write the document index TODO
    write dest $ M.fromList $ concat
        $ zipWith (\delta -> map (first (`applyDocIdDelta` delta))) docIds0 (map documents idxs)
    return docIds0
