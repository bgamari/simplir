{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module SimplIR.DiskIndex
    ( DiskIndex(..)
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
      -- * Typed wrapper
    , OnDiskIndex(..)
    , openOnDiskIndex
    ) where

import System.FilePath
import System.Directory
import Data.Binary
import qualified Data.Map as M
import Pipes (Producer)

import           SimplIR.Term
import           SimplIR.Types
import qualified SimplIR.DiskIndex.Posting as PostingIdx
import qualified SimplIR.DiskIndex.Posting.Types as PostingIdx
import qualified SimplIR.DiskIndex.Posting.Merge as PostingIdx.Merge
import qualified SimplIR.DiskIndex.Document as Doc

-- | @DiskIndex doc p@ is an on-disk index with document metadata @doc@
-- and posting-type @p@.
data DiskIndex doc p
    = DiskIndex { tfIdx  :: PostingIdx.DiskIndex p
                , docIdx :: Doc.DocIndex doc
                }

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: FilePath -> IO (DiskIndex doc p)
open path = do
    doc <- Doc.open $ Doc.DocIndexPath $ path </> "documents"
    Right tf <- PostingIdx.open $ PostingIdx.PostingIndexPath $ path </> "postings" -- TODO: Error handling
    return $ DiskIndex tf doc

-- | Build an on-disk index from a set of documents and their postings.
fromDocuments :: (Binary doc, Binary p)
              => FilePath                 -- ^ destination path
              -> [(DocumentId, doc)]  -- ^ document metadata and postings
              -> M.Map Term [Posting p]
              -> IO ()
fromDocuments dest docs postings = do
    createDirectoryIfMissing True dest
    PostingIdx.fromTermPostings postingChunkSize (PostingIdx.PostingIndexPath $ dest </> "postings") postings
    Doc.write (Doc.DocIndexPath $ dest </> "documents") (M.fromList docs)

documents :: (Monad m, Binary doc)
          => DiskIndex doc p -> Producer (DocumentId, doc) m ()
documents = Doc.documents . docIdx

-- | Lookup the metadata of a document.
lookupDoc :: (Binary doc)
          => DocumentId -> DiskIndex doc p -> Maybe doc
lookupDoc docId = Doc.lookupDoc docId . docIdx

-- | Lookup the 'Posting's of a 'Term' in the index.
lookupPostings' :: (Binary p)
                => Term                  -- ^ the term
                -> DiskIndex doc p
                -> Maybe [Posting p]     -- ^ the postings of the term
lookupPostings' term idx =
    PostingIdx.lookup (tfIdx idx) term

lookupPostings :: (Binary p, Binary doc)
               => Term                  -- ^ the term
               -> DiskIndex doc p
               -> Maybe [(doc, p)]  -- ^ the postings of the term
lookupPostings term idx =
    fmap (map lookupMeta) $ lookupPostings' term idx
  where
    lookupMeta p = (doc, postingBody p)
      where
        doc = case lookupDoc (postingDocId p) idx of
                Nothing -> error $ "Failed to find document "++show (postingDocId p)
                Just d  -> d

-- | Enumerate the postings for all terms in the index.
termPostings :: (Binary p)
             => DiskIndex doc p
             -> [(Term, [Posting p])]
termPostings idx =
    PostingIdx.walk (tfIdx idx)

-- | How many postings per chunk?
postingChunkSize :: Int
postingChunkSize = 2^(14 :: Int)

merge :: forall doc p. (Binary p, Binary doc)
      => FilePath              -- ^ destination path
      -> [DiskIndex doc p] -- ^ indices to merge
      -> IO ()
merge dest idxs = do
    createDirectoryIfMissing True dest
    -- First merge the document ids
    let docDest = Doc.DocIndexPath $ dest </> "documents"
    docIds0 <- Doc.merge docDest (map docIdx idxs)

    -- then merge the postings themselves
    let allPostings :: [[(Term, [PostingIdx.PostingsChunk p])]]
        allPostings = map (PostingIdx.walkChunks . tfIdx) idxs

    let mergedSize = sum $ map (PostingIdx.termCount . tfIdx) idxs
    PostingIdx.Merge.merge postingChunkSize
                           (PostingIdx.PostingIndexPath $ dest </> "postings") mergedSize
                           (zip docIds0 allPostings)

-- | A typed newtype wrapper
newtype OnDiskIndex doc p = OnDiskIndex { onDiskIndexPath :: FilePath }

openOnDiskIndex :: (Binary doc)
                => OnDiskIndex doc p
                -> IO (DiskIndex doc p)
openOnDiskIndex = open . onDiskIndexPath
