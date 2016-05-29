{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module DiskIndex
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
    ) where

import System.FilePath
import System.Directory
import Data.Bifunctor
import Data.Binary
import Data.Monoid
import Data.List (mapAccumL)
import qualified Data.Map as M

import Types
import qualified DiskIndex.Posting as PostingIdx
import qualified DiskIndex.Posting.Types as PostingIdx
import qualified DiskIndex.Posting.Merge as PostingIdx.Merge
import qualified DiskIndex.Document as Doc

-- | @DiskIndex docmeta p@ is an on-disk index with document metadata @docmeta@
-- and posting-type @p@.
data DiskIndex docmeta p
    = DiskIndex { tfIdx  :: PostingIdx.DiskIndex p
                , docIdx :: Doc.DocIndex docmeta
                }

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: (Binary docmeta, Binary p) => FilePath -> IO (DiskIndex docmeta p)
open path = do
    doc <- Doc.open $ path </> "documents"
    Right tf <- PostingIdx.open $ path </> "postings" -- TODO: Error handling
    return $ DiskIndex tf doc

-- | Build an on-disk index from a set of documents and their postings.
fromDocuments :: (Binary docmeta, Binary p)
              => FilePath                 -- ^ destination path
              -> [(DocumentId, docmeta)]  -- ^ document metadata and postings
              -> M.Map Term [Posting p]
              -> IO ()
fromDocuments dest docs postings = do
    createDirectoryIfMissing True dest
    PostingIdx.fromTermPostings postingChunkSize (dest </> "postings") postings
    Doc.write (dest </> "documents") (M.fromList docs)

documents :: DiskIndex docmeta p -> [(DocumentId, docmeta)]
documents = Doc.documents . docIdx

-- | Lookup the metadata of a document.
lookupDoc :: DocumentId -> DiskIndex docmeta p -> Maybe docmeta
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
postingChunkSize = 2^14

merge :: forall docmeta p. (Binary p, Binary docmeta)
      => FilePath              -- ^ destination path
      -> [DiskIndex docmeta p] -- ^ indices to merge
      -> IO ()
merge dest idxs = do
    createDirectoryIfMissing True dest
    -- First merge the document ids
    let docIds0 :: [PostingIdx.DocIdDelta]
        (_, docIds0) = mapAccumL (\docId0 idx -> (docId0 <> PostingIdx.toDocIdDelta (Doc.size $ docIdx idx), docId0))
                            (PostingIdx.DocIdDelta 0) idxs
    -- then write the document index TODO
    Doc.write (dest </> "documents") $ M.fromList $ concat
        $ zipWith (\delta -> map (first (`PostingIdx.applyDocIdDelta` delta))) docIds0 (map documents idxs)
    -- then merge the postings themselves
    let allPostings :: [[(Term, [PostingIdx.PostingsChunk p])]]
        allPostings = map (PostingIdx.walkChunks . tfIdx) idxs

    let mergedSize = sum $ map (PostingIdx.termCount . tfIdx) idxs
    PostingIdx.Merge.merge postingChunkSize
                           (dest </> "postings") mergedSize
                           (zip docIds0 allPostings)
