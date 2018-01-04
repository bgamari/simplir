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

import           SimplIR.Types
import qualified SimplIR.DiskIndex.Posting as PostingIdx
import qualified SimplIR.DiskIndex.Posting.Types as PostingIdx
import qualified SimplIR.DiskIndex.Posting.Merge as PostingIdx.Merge
import qualified SimplIR.DiskIndex.Document as Doc

-- | @DiskIndex term doc p@ is an on-disk index with document metadata @doc@
-- and posting-type @p@.
data DiskIndex term doc p
    = DiskIndex { tfIdx  :: PostingIdx.DiskIndex term p
                , docIdx :: Doc.DocIndex doc
                }

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: FilePath -> IO (DiskIndex term doc p)
open path = do
    doc <- Doc.open $ Doc.DocIndexPath $ path </> "documents"
    mtf <- PostingIdx.open $ PostingIdx.PostingIndexPath $ path </> "postings" -- TODO: Error handling
    case mtf of
      Right tf -> return $ DiskIndex tf doc
      Left err -> fail $ "SimplIR.DiskIndex.open: Failed to open index " ++ path ++ ": " ++ show err

-- | Build an on-disk index from a set of documents and their postings.
fromDocuments :: (Binary doc, Binary p, Binary term)
              => FilePath                 -- ^ destination path
              -> [(DocumentId, doc)]  -- ^ document metadata and postings
              -> M.Map term [Posting p]
              -> IO ()
fromDocuments dest docs postings = do
    createDirectoryIfMissing True dest
    PostingIdx.fromTermPostings postingChunkSize (PostingIdx.PostingIndexPath $ dest </> "postings") postings
    Doc.write (Doc.DocIndexPath $ dest </> "documents") (M.fromList docs)
{-# INLINEABLE fromDocuments #-}

documents :: (Monad m, Binary term, Binary doc)
          => DiskIndex term doc p -> Producer (DocumentId, doc) m ()
documents = Doc.documents . docIdx
{-# INLINEABLE documents #-}

-- | Lookup the metadata of a document.
lookupDoc :: (Binary doc)
          => DocumentId -> DiskIndex term doc p -> Maybe doc
lookupDoc docId = Doc.lookupDoc docId . docIdx
{-# INLINEABLE lookupDoc #-}

-- | Lookup the 'Posting's of a term in the index.
lookupPostings' :: (Binary p, Binary term, Ord term)
                => term                  -- ^ the term
                -> DiskIndex term doc p
                -> Maybe [Posting p]     -- ^ the postings of the term
lookupPostings' term idx =
    PostingIdx.lookup (tfIdx idx) term
{-# INLINEABLE lookupPostings' #-}

lookupPostings :: (Binary p, Binary doc, Binary term, Ord term)
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
termPostings :: (Binary p, Binary term)
             => DiskIndex term doc p
             -> [(term, [Posting p])]
termPostings idx =
    PostingIdx.walk (tfIdx idx)
{-# INLINEABLE termPostings #-}

-- | How many postings per chunk?
postingChunkSize :: Int
postingChunkSize = 2^(14 :: Int)

merge :: forall term doc p. (Binary p, Binary doc, Binary term, Ord term)
      => FilePath               -- ^ destination path
      -> [DiskIndex term doc p] -- ^ indices to merge
      -> IO ()
merge dest idxs = do
    createDirectoryIfMissing True dest
    -- First merge the document ids
    let docDest = Doc.DocIndexPath $ dest </> "documents"
    docIds0 <- Doc.merge docDest (map docIdx idxs)

    -- then merge the postings themselves
    let allPostings :: [[(term, [PostingIdx.PostingsChunk p])]]
        allPostings = map (PostingIdx.walkChunks . tfIdx) idxs

    let mergedSize = sum $ map (PostingIdx.termCount . tfIdx) idxs
    PostingIdx.Merge.merge postingChunkSize
                           (PostingIdx.PostingIndexPath $ dest </> "postings") mergedSize
                           (zip docIds0 allPostings)
{-# INLINEABLE merge #-}

-- | A typed newtype wrapper
newtype OnDiskIndex term doc p = OnDiskIndex { onDiskIndexPath :: FilePath }

openOnDiskIndex :: (Binary doc)
                => OnDiskIndex term doc p
                -> IO (DiskIndex term doc p)
openOnDiskIndex = open . onDiskIndexPath
