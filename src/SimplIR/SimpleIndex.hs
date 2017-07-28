{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}

-- | A simple wrapper around "SimplIR.DiskIndex".
module SimplIR.SimpleIndex
    ( -- * On disk
      OnDiskIndexWithStats(..)
      -- * Index
    , open
    , buildTermFreq
      -- * Querying
    , RetrievalModel
    , score
    , lookupPostings
    ) where

import Data.Maybe
import Data.Bifunctor

import Pipes
import qualified Pipes.Prelude as PP
import Pipes.Safe
import Data.Hashable
import qualified Data.HashSet as HS
import Data.Binary (Binary)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Map.Strict as M
import qualified Control.Foldl as Foldl
import qualified Data.Binary.Serialise.CBOR as S
import System.FilePath
import Numeric.Log

import SimplIR.Types (DocumentId, DocumentLength(..))
import SimplIR.DiskIndex.Build
import SimplIR.DiskIndex.Posting.Collect
import qualified SimplIR.DiskIndex as DiskIndex
import SimplIR.Term as Term
import SimplIR.RetrievalModels.CorpusStats as CorpusStats


newtype OnDiskIndexWithStats term doc posting
    = OnDiskIndexWithStats FilePath
    deriving (Eq, Ord, Show)


data IndexWithStats term doc posting
     = IndexWithStats { postingsIndex :: DiskIndex.DiskIndex (DocumentLength, doc) posting
                      , corpusStats   :: CorpusStats term
                      }


postingsPath :: OnDiskIndexWithStats term doc posting -> FilePath
postingsPath (OnDiskIndexWithStats f) = f <.> "postings"

statsPath :: OnDiskIndexWithStats term doc posting -> FilePath
statsPath (OnDiskIndexWithStats f) = f <.> "stats"

-- | Open an index.
open :: (Hashable term, Eq term, S.Serialise term)
     => OnDiskIndexWithStats term doc posting
     -> IO (IndexWithStats term doc posting)
open path = do
    postings <- DiskIndex.open (postingsPath path)
    stats <- S.deserialise <$> BSL.readFile (statsPath path)
    return (IndexWithStats postings stats)

-- | Build an index with term-frequency postings.
buildTermFreq :: forall doc term. (term ~ Term, Ord term, Binary doc)
              => FilePath              -- ^ output path
              -> [(doc, [term])]       -- ^ documents and their contents
              -> IO (OnDiskIndexWithStats term doc Int)
buildTermFreq path docs = do
    (stats, _idx) <-
        runSafeT $ Foldl.foldM ((,) <$> Foldl.generalize buildStats
                                    <*> buildPostings)
                               docs
    BSL.writeFile (statsPath path') $ S.serialise stats
    return path'
  where
    path' = OnDiskIndexWithStats path

    buildStats :: Foldl.Fold (doc, [term]) (CorpusStats term)
    buildStats = Foldl.premap snd (CorpusStats.documentTermStats Nothing)

    buildTfPostings :: (doc, [term]) -> ((DocumentLength, doc), M.Map term Int)
    buildTfPostings (doc, terms) =
        let termFreqs = M.fromListWith (+) [ (tok, 1) | tok <- terms ]
            docLength = DocLength $ Prelude.sum termFreqs
        in ((docLength, doc), termFreqs)

    buildPostings :: Foldl.FoldM (SafeT IO) (doc, [term])
                                 (DiskIndex.OnDiskIndex (DocumentLength, doc) Int)
    buildPostings = Foldl.premapM buildTfPostings (buildIndex 1024 (postingsPath path'))

lookupPostings :: forall term doc posting. (Binary posting, Binary doc, term ~ Term)
               => IndexWithStats term doc posting
               -> term
               -> [(doc, posting)]
lookupPostings index term =
      map (first snd)
    $ fromMaybe []
    $ DiskIndex.lookupPostings term (postingsIndex index)

-- | Query an index.
score :: forall term doc posting. (term ~ Term, Ord posting, Binary doc, Binary posting)
      => IndexWithStats term doc posting      -- ^ index
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


type RetrievalModel term doc posting
       = CorpusStats term      -- ^ corpus statistics
      -> [term]                -- ^ query terms
      -> doc                   -- ^ document being scored
      -> DocumentLength        -- ^ length of document
      -> [(term, posting)]     -- ^ document postings
      -> Log Double            -- ^ score
