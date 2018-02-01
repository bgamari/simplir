{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module SimplIR.DiskIndex
    ( DiskIndex(..)
    , DiskIndexPath(..)
      -- * Creation
    , open
    , fromDocuments
    , merge
      -- * Queries
    , lookupDoc
    , lookupPostings
    , lookupPostings'
    , termPostings
    , documents
      -- * Internal
    , postingIndexPath
    ) where

import Control.DeepSeq
import System.FilePath
import System.Directory
import Codec.Serialise (Serialise)
import qualified Data.Map as M

import           SimplIR.Types
import qualified SimplIR.DiskIndex.Posting2 as PostingIdx
import qualified SimplIR.DiskIndex.Posting2.TermIndex as TermIdx
import qualified SimplIR.DiskIndex.Posting2.Merge as PostingIdx.Merge
import qualified SimplIR.DiskIndex.Document as Doc

newtype DiskIndexPath term doc p = DiskIndexPath { getDiskIndexPath :: FilePath }

docIndexPath :: DiskIndexPath term doc p -> Doc.DocIndexPath doc
docIndexPath (DiskIndexPath p) = Doc.DocIndexPath $ p </> "documents"

postingIndexPath :: DiskIndexPath term doc p -> PostingIdx.PostingIndexPath term p
postingIndexPath (DiskIndexPath p) = PostingIdx.PostingIndexPath $ p </> "postings"

-- | @DiskIndex term doc p@ is an on-disk index with document metadata @doc@
-- and posting-type @p@.
data DiskIndex term doc p
    = DiskIndex { postingIdx :: !(PostingIdx.PostingIndex term p)
                , termIdx :: !(TermIdx.TermIndex term p)
                , docIdx :: !(Doc.DocIndex doc)
                }

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: forall term doc p.
        (Serialise doc, Serialise p, Serialise term, Ord term, NFData doc, NFData term)
     => DiskIndexPath term doc p -> IO (DiskIndex term doc p)
open path = do
    doc <- Doc.open $ docIndexPath path
    postings <- PostingIdx.open $ postingIndexPath path -- TODO: Error handling
    let term = TermIdx.build postings
    return $ DiskIndex postings term doc

-- | Build an on-disk index from a set of documents and their postings.
fromDocuments :: (Serialise doc, Serialise p, Serialise term)
              => FilePath                 -- ^ destination path
              -> [(DocumentId, doc)]  -- ^ document metadata and postings
              -> M.Map term [Posting p]
              -> IO (DiskIndexPath term doc p)
fromDocuments dest docs postings = do
    createDirectoryIfMissing True dest
    let path = DiskIndexPath dest
    _ <- PostingIdx.fromTermPostings postingChunkSize (PostingIdx.getPostingIndexPath $ postingIndexPath path) postings
    _ <- Doc.write (Doc.getDocIndexPath $ docIndexPath path) (M.fromList docs)
    return path
{-# INLINEABLE fromDocuments #-}

documents :: (Serialise term, Serialise doc)
          => DiskIndex term doc p -> [(DocumentId, doc)]
documents = Doc.documents . docIdx
{-# INLINEABLE documents #-}

-- | Lookup the metadata of a document.
lookupDoc :: (Serialise doc)
          => DocumentId -> DiskIndex term doc p -> Maybe doc
lookupDoc docId = Doc.lookupDoc docId . docIdx
{-# INLINEABLE lookupDoc #-}

-- | Lookup the 'Posting's of a term in the index.
lookupPostings' :: (Serialise p, Serialise term, Ord term)
                => term                  -- ^ the term
                -> DiskIndex term doc p
                -> Maybe [Posting p]     -- ^ the postings of the term
lookupPostings' term idx =
    TermIdx.lookup (termIdx idx) term
{-# INLINEABLE lookupPostings' #-}

lookupPostings :: (Serialise p, Serialise doc, Serialise term, Ord term)
               => term                  -- ^ the term
               -> DiskIndex term doc p
               -> Maybe [(doc, p)]  -- ^ the postings of the term
lookupPostings term idx =
    fmap (map lookupMeta) $ lookupPostings' term idx
  where
    lookupMeta p = (doc, postingBody p)
      where
        doc = case lookupDoc (postingDocId p) idx of
                Nothing -> error $ "Failed to find document "++show (postingDocId p)
                Just d  -> d
{-# INLINEABLE lookupPostings #-}

-- | Enumerate the postings for all terms in the index.
termPostings :: (Serialise p, Serialise term)
             => DiskIndex term doc p
             -> [(term, [Posting p])]
termPostings idx =
    PostingIdx.toPostingsLists (postingIdx idx)
{-# INLINEABLE termPostings #-}

-- | How many postings per chunk?
postingChunkSize :: Int
postingChunkSize = 2^(14 :: Int)

merge :: forall term doc p.
         (Serialise p, Serialise doc, Serialise term, Ord term, NFData doc)
      => FilePath               -- ^ destination path
      -> [DiskIndexPath term doc p] -- ^ indices to merge
      -> IO (DiskIndexPath term doc p)
merge dest idxs = do
    let path = DiskIndexPath dest
    createDirectoryIfMissing True dest
    -- First merge the document ids
    docIdxs <- mapM (Doc.open . docIndexPath) idxs
    docIds0 <- Doc.merge (docIndexPath path) docIdxs

    -- then merge the postings themselves
    _ <- PostingIdx.Merge.merge (PostingIdx.getPostingIndexPath $ postingIndexPath path)
        [ (docId0, postingIndexPath idxPath)
        | (docId0, idxPath) <- zip docIds0 idxs
        ]
    return path
{-# INLINEABLE merge #-}
