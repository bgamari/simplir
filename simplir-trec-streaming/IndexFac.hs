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
import qualified Data.Vector.Generic as V.G

import           Pipes
import           Pipes.Safe
import qualified Pipes.Text.Encoding as P.T
import qualified Pipes.Prelude as P.P

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import SimplIR.Utils
import Control.Foldl.Map
import SimplIR.Types
import SimplIR.Term as Term
import SimplIR.Tokenise
import SimplIR.DataSource
import qualified BTree.File as BTree
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
                foldChunks 100000 (Foldl.generalize $ indexPostings id)
             $  facEntities dsrcs
            >-> cat'                                              @(DocumentInfo, [Term])
            >-> P.P.map (second $ \postings -> foldTokens Foldl.mconcat $ zip postings (repeat $ TermFreq 1))
            >-> cat'                                              @( DocumentInfo
                                                                   , M.Map Term TermFrequency
                                                                   )

    chunks <- runSafeT $ P.P.toListM $ for (chunkIndexes >-> zipWithList [0..]) $ \(n, (docs, termFreqs, corpusStats)) -> do
        let indexPath = "index-"++show n
        liftIO $ createDirectoryIfMissing True indexPath
        liftIO $ print (n, M.size docs)

        let docsPath :: BTree.BTreePath DocumentName (DocumentInfo, M.Map Term TermFrequency)
            docsPath = BTree.BTreePath $ indexPath </> "documents"
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size docs) docsPath (each $ M.toAscList docs)

        let termFreqsPath :: BTree.BTreePath Term (TermFrequency, DocumentFrequency)
            termFreqsPath = BTree.BTreePath $ indexPath </> "term-stats"
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size termFreqs) termFreqsPath (each $ M.toAscList termFreqs)

        yield (docsPath, termFreqsPath, corpusStats)

    mergeIndexes chunks

mergeIndexes :: [( BTree.BTreePath DocumentName (DocumentInfo, M.Map Term TermFrequency)
                 , BTree.BTreePath Term (TermFrequency, DocumentFrequency)
                 , CorpusStats
                 )]
             -> IO ()
mergeIndexes chunks = do
    createDirectoryIfMissing True "index"
    putStrLn "merging"
    BTree.merge (const id) (BTree.BTreePath "index/documents") $ map (\(docs,_,_) -> docs) chunks
    putStrLn "documents done"
    BTree.merge mappend (BTree.BTreePath "index/term-stats") $ map (\(_,x,_) -> x) chunks
    putStrLn "term stats done"
    BS.L.writeFile ("index" </> "corpus-stats") $ encode $ foldMap (\(_,_,x) -> x) chunks
    return ()

type FragmentIndex p =
    ( M.Map DocumentName (DocumentInfo, M.Map Term p)
    , M.Map Term (TermFrequency, DocumentFrequency)
    , CorpusStats
    )

indexPostings :: forall p. ()
              => (p -> TermFrequency)
              -> Fold (DocumentInfo, M.Map Term p)
                      (FragmentIndex p)
indexPostings getTermFreq =
    (,,)
      <$> docMeta
      <*> lmap snd termFreqs
      <*> lmap fst foldCorpusStats
  where
    docMeta :: Fold (DocumentInfo, M.Map Term p) (M.Map DocumentName (DocumentInfo, M.Map Term p))
    docMeta  = lmap (\(docInfo, terms) -> M.singleton (docName docInfo) (docInfo, terms)) Foldl.mconcat

    termFreqs :: Fold (M.Map Term p) (M.Map Term (TermFrequency, DocumentFrequency))
    termFreqs = lmap M.assocs
                $ Foldl.handles traverse
                $ lmap (\(term, ps) -> M.singleton term (getTermFreq ps, DocumentFrequency 1))
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
