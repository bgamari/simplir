{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveGeneric #-}

import Control.Monad.State.Strict hiding ((>=>))
import Data.Bifunctor
import Data.Foldable (toList)
import Data.Maybe
import Data.Monoid
import Data.Profunctor
import GHC.Generics
import System.FilePath
import System.Directory (createDirectoryIfMissing)

import Data.Binary
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Control.Foldl as Foldl
import           Control.Foldl (Fold)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as V.G
import           Data.Vector.Algorithms.Heap (sort)

import           Pipes
import           Pipes.Safe
import qualified Pipes.Text.Encoding as P.T
import qualified Pipes.Prelude as P.P

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import SimplIR.Utils
import AccumPostings
import Control.Foldl.Map
import SimplIR.Types
import SimplIR.Term as Term
import SimplIR.Tokenise
import SimplIR.DataSource
import qualified BTree.File as BTree
import qualified SimplIR.DiskIndex.Posting as PostingIdx
import qualified SimplIR.DiskIndex.Document as DocIdx
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac

inputFiles :: Parser (IO [DataSource])
inputFiles =
    concatThem <$> some (argument (parse <$> str) (metavar "FILE" <> help "TREC input file"))
  where
    concatThem :: [IO [DataSource]] -> IO [DataSource]
    concatThem = fmap concat . sequence

    parse :: String -> IO [DataSource]
    parse ('@':rest) = map parse' . lines <$> readFile rest
    parse fname      = return [parse' fname]
    parse'           = fromMaybe (error "unknown input file type") . parseDataSource . T.pack

facEntities :: [DataSource]
            -> Producer (DocumentInfo, [Term]) (SafeT IO) ()
facEntities dsrcs =
    mapM_ (\dsrc -> Fac.parseDocuments (P.T.decodeUtf8 $ dataSource dsrc)
                    >-> P.P.map (\d -> ( DocInfo (Fac.docArchive d)
                                                 (DocName $ Utf8.fromText $ Fac.docId d)
                                                 (DocLength $ length $ Fac.docAnnotations d)
                                       , map (Term.fromText . Fac.getEntityId . Fac.annEntity) (Fac.docAnnotations d)))
          ) dsrcs

buildIndex :: IO [DataSource] -> IO ()
buildIndex readDocLocs = do
    dsrcs <- readDocLocs

    let chunkIndexes :: Producer (FragmentIndex TermFrequency) (SafeT IO) ()
        chunkIndexes =
                foldChunks 10000 (Foldl.generalize $ indexPostings id)
             $  facEntities dsrcs
            >-> cat'                                              @(DocumentInfo, [Term])
            >-> zipWithList [DocId 0..]
            >-> cat'                                              @(DocumentId, (DocumentInfo, [Term]))
            >-> P.P.map (\(docId, (info, postings)) -> ((docId, info), (docId, postings)))
            >-> P.P.map (second $ \(docId, postings) ->
                            toPostings docId $ M.assocs $ foldTokens Foldl.mconcat $ zip postings (repeat $ TermFreq 1)
                        )
            >-> cat'                                              @( (DocumentId, DocumentInfo)
                                                                   , [(Term, Posting TermFrequency)]
                                                                   )

    chunks <- runSafeT $ P.P.toListM $ for (chunkIndexes >-> zipWithList [0..]) $ \(n, (docIds, postings, termFreqs, corpusStats)) -> do
        let indexPath = "index-"++show n
        liftIO $ createDirectoryIfMissing True indexPath
        liftIO $ print (n, M.size docIds)

        let postingsPath :: PostingIdx.PostingIndexPath TermFrequency
            postingsPath = PostingIdx.PostingIndexPath $ indexPath </> "postings"
        liftIO $ PostingIdx.fromTermPostings 1024 postingsPath (fmap V.toList postings)

        let docsPath :: DocIdx.DocIndexPath DocumentInfo
            docsPath = DocIdx.DocIndexPath $ indexPath </> "documents"
        liftIO $ DocIdx.write docsPath docIds

        let termFreqsPath :: BTree.BTreePath Term (TermFrequency, DocumentFrequency)
            termFreqsPath = BTree.BTreePath $ indexPath </> "term-stats"
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size termFreqs) termFreqsPath (each $ M.toList termFreqs)

        yield (postingsPath, docsPath, termFreqsPath, corpusStats)

    mergeIndexes chunks

mergeIndexes :: [( PostingIdx.PostingIndexPath TermFrequency
                 , DocIdx.DocIndexPath DocumentInfo
                 , BTree.BTreePath Term (TermFrequency, DocumentFrequency)
                 , CorpusStats
                 )]
             -> IO ()
mergeIndexes chunks = do
    createDirectoryIfMissing True "index"
    putStrLn "merging"
    docIds0 <- DocIdx.merge (DocIdx.DocIndexPath "index/documents") $ map (\(_,docs,_,_) -> docs) chunks
    putStrLn "documents done"
    PostingIdx.merge (PostingIdx.PostingIndexPath "index/postings") $ zip docIds0 $ map (\(postings,_,_,_) -> postings) chunks
    putStrLn "postings done"
    BTree.merge mappend (BTree.BTreePath "index/term-stats") $ map (\(_,_,x,_) -> x) chunks
    putStrLn "term stats done"
    BS.L.writeFile ("index" </> "corpus-stats") $ encode $ foldMap (\(_,_,_,x) -> x) chunks
    return ()

type SavedPostings p = M.Map Term (V.Vector (Posting p))
type FragmentIndex p =
    ( M.Map DocumentId DocumentInfo
    , SavedPostings p
    , M.Map Term (TermFrequency, DocumentFrequency)
    , CorpusStats
    )

indexPostings :: forall p. (Ord p)
              => (p -> TermFrequency)
              -> Fold ((DocumentId, DocumentInfo), [(Term, Posting p)])
                      (FragmentIndex p)
indexPostings getTermFreq =
    (,,,)
      <$> lmap fst docMeta
      <*> lmap snd postings
      <*> lmap snd termFreqs
      <*> lmap (snd . fst) foldCorpusStats
  where
    docMeta  = lmap (\(docId, docInfo) -> M.singleton docId docInfo) Foldl.mconcat
    postings = fmap (M.map $ V.G.modify sort . fromFoldable) foldPostings

    termFreqs :: Fold [(Term, Posting p)] (M.Map Term (TermFrequency, DocumentFrequency))
    termFreqs = Foldl.handles traverse
                $ lmap (\(term, ps) -> M.singleton term (getTermFreq $ postingBody ps, DocumentFrequency 1))
                $ mconcatMaps

foldCorpusStats :: Foldl.Fold DocumentInfo CorpusStats
foldCorpusStats =
    CorpusStats
      <$> lmap (fromEnum . docLength) Foldl.sum
      <*> Foldl.length

data DocumentInfo = DocInfo { docArchive :: ArchiveName
                            , docName    :: DocumentName
                            , docLength  :: DocumentLength
                            }
                  deriving (Generic, Eq, Ord)
instance Binary DocumentInfo

newtype DocumentFrequency = DocumentFrequency Int
                          deriving (Show, Eq, Ord, Binary)
instance Monoid DocumentFrequency where
    mempty = DocumentFrequency 0
    DocumentFrequency a `mappend` DocumentFrequency b = DocumentFrequency (a+b)

type ArchiveName = T.Text

data CorpusStats = CorpusStats { corpusCollectionLength :: !Int
                                 -- ^ How many tokens in collection
                               , corpusCollectionSize   :: !Int
                                 -- ^ How many documents in collection
                               }
                 deriving (Generic)

instance Binary CorpusStats
instance Monoid CorpusStats where
    mempty = CorpusStats 0 0
    a `mappend` b =
        CorpusStats { corpusCollectionLength = corpusCollectionLength a + corpusCollectionLength b
                    , corpusCollectionSize = corpusCollectionSize a + corpusCollectionSize b
                    }

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> indexMode) fullDesc
    mode

indexMode :: Parser (IO ())
indexMode =
    buildIndex <$> inputFiles

fromFoldable :: (Foldable f, V.G.Vector v a)
             => f a -> v a
fromFoldable xs = V.G.fromListN (length xs) (toList xs)
