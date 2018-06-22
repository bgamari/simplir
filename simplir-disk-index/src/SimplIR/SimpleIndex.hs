{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}

-- | A simple wrapper around "SimplIR.DiskIndex".
module SimplIR.SimpleIndex
    ( -- * On disk
      OnDiskIndex(..)
      -- * Index
    , Index
    , open
    , buildTermFreq
      -- * Querying
    , RetrievalModel
    , score
    , lookupPostings
    , termPostings
    , descending
    ) where

import Data.Maybe
import Data.Bifunctor
import Control.DeepSeq

import Pipes
import qualified Pipes.Prelude as PP
import Pipes.Safe
import Data.Hashable
import qualified Data.HashSet as HS
import Data.Ord
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Map.Strict as M
import qualified Control.Foldl as Foldl
import Codec.Serialise (Serialise)
import qualified Codec.Serialise as S
import System.FilePath
import System.Directory (createDirectory)
import Numeric.Log

import SimplIR.Types (DocumentId, DocumentLength(..), Posting(..))
import SimplIR.DiskIndex.Build
import SimplIR.DiskIndex.Posting.Collect
import qualified SimplIR.DiskIndex as DiskIndex
import SimplIR.RetrievalModels.CorpusStats as CorpusStats
import SimplIR.Utils.Compact


newtype OnDiskIndex term doc posting
    = OnDiskIndex FilePath
    deriving (Eq, Ord, Show)


data Index term doc posting
     = Index { postingsIndex :: DiskIndex.DiskIndex term (DocumentLength, doc) posting
             , corpusStats   :: CorpusStats term
             }

simpleIndexPath :: OnDiskIndex term doc posting
                -> DiskIndex.DiskIndexPath term (DocumentLength, doc) posting
simpleIndexPath (OnDiskIndex f) = DiskIndex.DiskIndexPath $ f </> "index"

statsPath :: OnDiskIndex term doc posting -> FilePath
statsPath (OnDiskIndex f) = f </> "stats"

-- | Open an index.
open :: (Ord term, Hashable term, Eq term, Serialise term, Serialise posting, Serialise doc, NFData doc, NFData term)
     => OnDiskIndex term doc posting
     -> IO (Index term doc posting)
open path = do
    postings <- DiskIndex.open (simpleIndexPath path)
    stats <- inCompactM (either uhOh id . S.deserialiseOrFail <$> BSL.readFile (statsPath path))
    return (Index postings stats)
  where uhOh err = error $ "Error: SimpleIndex: Failed to deserialise statistics: "++show err

-- | Build an index with term-frequency postings.
buildTermFreq :: forall doc term. (Ord term, Hashable term, Serialise term, Serialise term, Serialise doc, NFData doc)
              => FilePath              -- ^ output path
              -> [(doc, [term])]       -- ^ documents and their contents
              -> IO (OnDiskIndex term doc Int)
buildTermFreq path docs = do
    createDirectory path
    (stats, _idx) <-
        runSafeT $ Foldl.foldM ((,) <$> Foldl.generalize buildStats
                                    <*> buildPostings)
                               docs
    BSL.writeFile (statsPath path') $ S.serialise stats
    return path'
  where
    path' = OnDiskIndex path

    buildStats :: Foldl.Fold (doc, [term]) (CorpusStats term)
    buildStats = Foldl.premap snd (CorpusStats.documentTermStats Nothing)

    buildTfPostings :: (doc, [term]) -> ((DocumentLength, doc), M.Map term Int)
    buildTfPostings (doc, terms) =
        let termFreqs = M.fromListWith (+) [ (tok, 1) | tok <- terms ]
            docLength = DocLength $ Prelude.sum termFreqs
        in ((docLength, doc), termFreqs)

    chunkSize = 64000 -- TODO: make configurable
    buildPostings :: Foldl.FoldM (SafeT IO) (doc, [term])
                                 (DiskIndex.DiskIndexPath term (DocumentLength, doc) Int)
    buildPostings =
        Foldl.premapM buildTfPostings
        $ buildIndex chunkSize destPath
      where destPath = DiskIndex.getDiskIndexPath $ simpleIndexPath path'
{-# INLINEABLE buildTermFreq #-}

termPostings :: forall term doc posting. (Ord term, Serialise term, Serialise posting, Serialise doc)
             => Index term doc posting
             -> [(term, [(doc, posting)])]
termPostings idx = map (second $ map toPair) $ DiskIndex.termPostings (postingsIndex idx)
  where toPair (Posting docId p) = (doc, p)
          where Just (_docLen, doc) = DiskIndex.lookupDoc docId (postingsIndex idx)
{-# INLINEABLE termPostings #-}

lookupPostings :: forall term doc posting. (Ord term, Serialise term, Serialise posting, Serialise doc)
               => Index term doc posting
               -> term
               -> [(doc, posting)]
lookupPostings index term =
      map (first snd)
    $ fromMaybe []
    $ DiskIndex.lookupPostings term (postingsIndex index)
{-# INLINEABLE lookupPostings #-}

-- | Query an index, returning un-sorted results.
score :: forall term doc posting. (Hashable term, Ord term, Serialise term, Serialise doc, Serialise posting, Ord posting)
      => Index term doc posting               -- ^ index
      -> RetrievalModel term doc posting      -- ^ retrieval model
      -> [term]                               -- ^ query terms
      -> [(Log Double, doc)]
score index model =
    \queryTerms ->
        let postings :: [(DocumentId, [(term, posting)])]
            postings =
                  PP.toList $ collectPostings
                [ ( term
                  , Pipes.each $ fromMaybe []
                    $ DiskIndex.lookupPostings' term (postingsIndex index)
                  )
                | term <- HS.toList $ HS.fromList queryTerms
                ]
        in [ (scoreDoc queryTerms doc docLength docPostings, doc)
           | (docId, docPostings) <- postings
           , Just (docLength, doc) <- pure $ DiskIndex.lookupDoc docId (postingsIndex index)
           ]
  where
    !scoreDoc = model (corpusStats index)
{-# INLINEABLE score #-}


descending :: forall doc.
            (Log Double, doc) -> (Log Double, doc) -> Ordering
descending = flip $ comparing fst
{-# INLINEABLE descending #-}


type RetrievalModel term doc posting
       = CorpusStats term      -- ^ corpus statistics
      -> [term]                -- ^ query terms
      -> doc                   -- ^ document being scored
      -> DocumentLength        -- ^ length of document
      -> [(term, posting)]     -- ^ document postings
      -> Log Double            -- ^ score
