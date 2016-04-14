{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module DiskIndex
    ( DiskIndex
      -- * Creation
    , open
    , fromDocuments
    , merge
      -- * Queries
    , lookupDoc
    , lookupPostings
    ) where

import System.FilePath
import Data.Binary
import Data.Monoid
import Data.List (mapAccumL)
import qualified Data.Heap as H
import qualified Data.Map as M

import Types
import qualified DiskIndex.TermFreq as TF
import qualified DiskIndex.TermFreq.Types as TF
import qualified DiskIndex.TermFreq.Merge as TF.Merge
import qualified DiskIndex.Document as Doc

-- | @DiskIndex docmeta p@ is an on-disk index with document metadata @docmeta@
-- and posting-type @p@.
data DiskIndex docmeta p
    = DiskIndex { tfIdx  :: TF.DiskIndex p
                , docIdx :: Doc.DocIndex docmeta
                }

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: (Binary docmeta, Binary p) => FilePath -> IO (DiskIndex docmeta p)
open path = do
    doc <- Doc.open $ path </> "documents"
    Right tf <- TF.open $ path </> "term-freq" -- TODO: Error handling
    return $ DiskIndex tf doc

-- | Build an on-disk index from a set of documents and their postings.
fromDocuments :: (Binary docmeta, Binary p)
              => FilePath                 -- ^ destination path
              -> [(DocumentId, docmeta)]  -- ^ document metadata and postings
              -> M.Map Term [Posting p]
              -> IO ()
fromDocuments dest docs postings = do
   TF.fromTermPostings chunkSize (dest </> "term-freq") postings
   Doc.write (dest </> "documents") (M.fromList docs)

-- | Lookup the metadata of a document.
lookupDoc :: DocumentId -> DiskIndex docmeta p -> Maybe docmeta
lookupDoc docId = Doc.lookupDoc docId . docIdx

-- | Lookup the 'Posting's of a 'Term' in the index.
lookupPostings :: (Binary p)
               => Term                  -- ^ the term
               -> DiskIndex docmeta p
               -> Maybe [Posting p]     -- ^ the postings of the term
lookupPostings term idx =
    TF.lookup (tfIdx idx) term

chunkSize = 10000

merge :: forall docmeta p. Binary p
      => FilePath            -- ^ destination path
      -> [DiskIndex docmeta p] -- ^ indices to merge
      -> IO ()
merge dest idxs = do
    -- First merge the document ids
    let docIds0 :: [TF.DocIdDelta]
        (_, docIds0) = mapAccumL (\docId0 idx -> (docId0 <> TF.toDocIdDelta (Doc.size $ docIdx idx), docId0))
                            (TF.DocIdDelta 0) idxs
    let allPostings :: [[(Term, [TF.PostingsChunk p])]]
        allPostings = map (TF.walkChunks . tfIdx) idxs

        mergeChunks = id -- TODO: Merge small chunks

    TF.Merge.merge (dest </> "term-freqs") chunkSize
                   (zip docIds0 allPostings)

    --writeIndex (dest </> "term-freqs") chunkSize
    --    $ mergeChunks
    --    $ interleavePostings
    --    $ zipWith (\delta terms -> map . fmap . map . applyDocIdDeltaToChunk delta $ terms)
    --              docIds0 allPostings


{-


-- | Given a set of $n$ sorted @[Entry p a]@s, lazily interleave them in sorted
-- order.
heapMerge :: forall p a. Ord p
      => [[H.Entry p a]] -> [H.Entry p a]
heapMerge = go . foldMap takeFirst
  where
    takeFirst :: [H.Entry p a] -> H.Heap (H.Entry p (a, [H.Entry p a]))
    takeFirst (H.Entry p x : rest) = H.singleton $ H.Entry p (x : rest)
    takeFirst []                   = H.empty

    go :: H.Heap (H.Entry p (a, [H.Entry p a]))
       -> [H.Entry p (a, [H.Entry p a])]
    go xs
      | H.null xs = []
      | otherwise =
        let (H.Entry p (x, rest), xs') = H.viewMin xs
        in H.Entry p x : go (xs' <> takeFirst rest)

interleavePostings :: [[(Term, [PostingsChunk p])]]
                   -> [(Term, [PostingsChunk p])]
interleavePostings = mergeTerms . heapMerge . map (map (curry H.Entry))
  where
    -- Merge chunks belonging to the same term
    mergeTerms :: [H.Entry Term [PostingsChunk p]] -> [(Term, [PostingsChunk p])]
    mergeTerms [] = []
    mergeTerms xs@(H.Entry term chunks : _) =
        let (postingsOfTerm, rest) = span (\(H.Entry term' _) -> term == term') xs
        in ( term
           , map chunkPostings
             $ sortBy (comparing chunkInitialDocId) postingsOfTerm
           )
-}
