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
import Data.Maybe
import Data.Monoid
import Data.Profunctor
import GHC.Generics
import System.FilePath
import System.Directory (createDirectoryIfMissing, removeDirectoryRecursive)
import System.IO.Temp

import Data.Binary
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Control.Foldl as Foldl
import           Control.Foldl (Fold)

import           Pipes
import           Pipes.Safe
import qualified Pipes.Text.Encoding as P.T
import qualified Pipes.Prelude as P.P

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import SimplIR.Utils
import Control.Foldl.Map
import SimplIR.BinaryFile as BinaryFile
import SimplIR.Types
import SimplIR.Term as Term
import SimplIR.Tokenise
import SimplIR.DataSource
import qualified BTree.File as BTree
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

modes :: Parser (IO ())
modes = subparser
    $  command "index" (info indexMode fullDesc)
    <> command "merge" (info mergeIndexesMode fullDesc)
    <> command "query" (info queryMode fullDesc)

indexMode :: Parser (IO ())
indexMode =
    buildIndex
      <$> option (diskIndexPaths <$> str) (help "output index" <> short 'o' <> long "output")
      <*> inputFiles

mergeIndexesMode :: Parser (IO ())
mergeIndexesMode =
    mergeIndexes
      <$> option (diskIndexPaths <$> str) (help "output index" <> short 'o' <> long "output")
      <*> some (argument (diskIndexPaths <$> str) (help "indexes to merge"))

queryMode :: Parser (IO ())
queryMode =
    query
      <$> option (diskIndexPaths <$> str) (help "index to query" <> short 'i' <> long "index")
      <*> argument (DocName . Utf8.fromString <$> str) (help "document names to find")
  where
    query :: DiskIndex -> DocumentName -> IO ()
    query index docName = do
        docs <- BTree.open (diskDocuments index)
        print $ BTree.lookup docs docName

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

buildIndex :: DiskIndex -> IO [DataSource] -> IO ()
buildIndex output readDocLocs = do
    dsrcs <- readDocLocs

    let chunkIndexes :: Producer (FragmentIndex TermFrequency) (SafeT IO) ()
        chunkIndexes =
                foldChunks 50000 (Foldl.generalize $ indexPostings id)
             $  facEntities dsrcs
            >-> cat'                                              @(DocumentInfo, [Term])
            >-> P.P.map (second $ \postings -> foldTokens Foldl.mconcat $ zip postings (repeat $ TermFreq 1))
            >-> cat'                                              @( DocumentInfo
                                                                   , M.Map Term TermFrequency
                                                                   )

    chunks <- runSafeT $ P.P.toListM $ for (chunkIndexes >-> zipWithList [0..]) $ \(n, (docs, termFreqs, corpusStats)) -> do
        liftIO $ print (n, M.size docs)
        root <- liftIO $ createTempDirectory "." "index"
        let indexPaths = diskIndexPaths root

        let docsPath :: BTree.BTreePath DocumentName (DocumentInfo, M.Map Term TermFrequency)
            docsPath = diskDocuments indexPaths
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size docs) docsPath (each $ M.toAscList docs)

        let termFreqsPath :: BTree.BTreePath Term (TermFrequency, DocumentFrequency)
            termFreqsPath = diskTermStats indexPaths
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size termFreqs) termFreqsPath (each $ M.toAscList termFreqs)

        liftIO $ BinaryFile.write (diskCorpusStats indexPaths) corpusStats
        yield indexPaths

    mergeIndexes output chunks
    mapM_ (removeDirectoryRecursive . diskRootDir) chunks

mergeIndexes :: DiskIndex
             -> [DiskIndex]
             -> IO ()
mergeIndexes output chunks = do
    createDirectoryIfMissing True (diskRootDir output)
    putStrLn "merging"
    BTree.merge (const id) (diskDocuments output) $ map diskDocuments chunks
    putStrLn "documents done"
    BTree.merge mappend (diskTermStats output) $ map diskTermStats chunks
    putStrLn "term stats done"
    corpusStats <- BinaryFile.mconcat $ map diskCorpusStats chunks
    BinaryFile.write (diskCorpusStats output) corpusStats
    return ()

diskIndexPaths :: FilePath -> DiskIndex
diskIndexPaths root =
    DiskIndex { diskRootDir     = root
              , diskDocuments   = BTree.BTreePath $ root </> "documents"
              , diskTermStats   = BTree.BTreePath $ root </> "term-stats"
              , diskCorpusStats = BinaryFile.BinaryFile $ root </> "corpus-stats"
              }

-- | Paths to the parts of an index on disk
data DiskIndex = DiskIndex { diskRootDir     :: FilePath
                           , diskDocuments   :: BTree.BTreePath DocumentName (DocumentInfo, M.Map Term TermFrequency)
                           , diskTermStats   :: BTree.BTreePath Term (TermFrequency, DocumentFrequency)
                           , diskCorpusStats :: BinaryFile CorpusStats
                           }

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
                  deriving (Generic, Eq, Ord, Show)
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
