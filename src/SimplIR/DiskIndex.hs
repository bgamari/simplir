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

-- | @DiskIndex docmeta p@ is an on-disk index with document metadata @docmeta@
-- and posting-type @p@.
data DiskIndex docmeta p
    = DiskIndex { tfIdx  :: PostingIdx.DiskIndex p
                , docIdx :: Doc.DocIndex docmeta
                }

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: FilePath -> IO (DiskIndex docmeta p)
open path = do
    doc <- Doc.open $ Doc.DocIndexPath $ path </> "documents"
    Right tf <- PostingIdx.open $ PostingIdx.PostingIndexPath $ path </> "postings" -- TODO: Error handling
    return $ DiskIndex tf doc

-- | Build an on-disk index from a set of documents and their postings.
fromDocuments :: (Binary docmeta, Binary p)
              => FilePath                 -- ^ destination path
              -> [(DocumentId, docmeta)]  -- ^ document metadata and postings
              -> M.Map Term [Posting p]
              -> IO ()
fromDocuments dest docs postings = do
    createDirectoryIfMissing True dest
    PostingIdx.fromTermPostings postingChunkSize (PostingIdx.PostingIndexPath $ dest </> "postings") postings
    Doc.write (Doc.DocIndexPath $ dest </> "documents") (M.fromList docs)

documents :: (Monad m, Binary docmeta)
          => DiskIndex docmeta p -> Producer (DocumentId, docmeta) m ()
documents = Doc.documents . docIdx

-- | Lookup the metadata of a document.
lookupDoc :: (Binary docmeta)
          => DocumentId -> DiskIndex docmeta p -> Maybe docmeta
lookupDoc docId = Doc.lookupDoc docId . docIdx

-- | Lookup the 'Posting's of a 'Term' in the index.
lookupPostings :: (Binary p)
               => Term                  -- ^ the term
               -> DiskIndex docmeta p
               -> Maybe [Posting p]     -- ^ the postings of the term
lookupPostings term idx =
    PostingIdx.lookup (tfIdx idx) term

-- | Enumerate the postings for all terms in the index.
termPostings :: (Binary p)
             => DiskIndex docmeta p
             -> [(Term, [Posting p])]
termPostings idx =
    PostingIdx.walk (tfIdx idx)

-- | How many postings per chunk?
postingChunkSize :: Int
postingChunkSize = 2^(14 :: Int)

merge :: forall docmeta p. (Binary p, Binary docmeta)
      => FilePath              -- ^ destination path
      -> [DiskIndex docmeta p] -- ^ indices to merge
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
newtype OnDiskIndex docmeta p = OnDiskIndex { onDiskIndexPath :: FilePath }

openOnDiskIndex :: (Binary docmeta)
                => OnDiskIndex docmeta p
                -> IO (DiskIndex docmeta p)
openOnDiskIndex = open . onDiskIndexPath
