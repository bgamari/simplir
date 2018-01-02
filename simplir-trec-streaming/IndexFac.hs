{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Monad.State.Strict hiding ((>=>))
import Data.Bifunctor
import Data.Maybe
import Data.Monoid
import Data.Profunctor
import Data.Foldable
import System.Directory (createDirectoryIfMissing, removeDirectoryRecursive)
import System.IO.Temp

import qualified Data.Aeson as Json
import qualified Data.ByteString.Lazy.Char8 as BSL
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
import SimplIR.Tokenise
import SimplIR.DataSource
import qualified BTree.File as BTree
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac
import SimplIR.TrecStreaming.FacAnnotations (EntityId)
import Fac.Types

import qualified BTree as BT

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

modes :: Parser (IO ())
modes = subparser
    $  command "index" (info indexMode fullDesc)
    <> command "merge" (info mergeIndexesMode fullDesc)
    <> command "query" (info queryMode fullDesc)
    <> command "list"  (info listMode fullDesc)

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

listMode :: Parser (IO ())
listMode =
    list <$> option (diskIndexPaths <$> str) (help "index to list" <> short 'i' <> long "index")
  where
    list :: DiskIndex -> IO ()
    list index = do
        docs <- BTree.open (diskDocuments index)
        _ <- runEffect $ for (BT.walkLeaves docs) (liftIO . print)
        return ()

queryMode :: Parser (IO ())
queryMode =
    query
      <$> option (diskIndexPaths <$> str) (help "index to query" <> short 'i' <> long "index")
      <*> argument (DocName . Utf8.fromString <$> str) (help "document names to find")
  where
    query :: DiskIndex -> DocumentName -> IO ()
    query index docName = do
        docs <- BTree.open (diskDocuments index)
        traverse_ (BSL.putStrLn . Json.encode . encodeDoc) $ BTree.lookup docs docName
    encodeDoc (DocInfo {..}, entities) = Json.object
        [ "archive"  Json..= docArchive
        , "id"       Json..= docName
        , "length"   Json..= docLength
        , "entities" Json..= entities
        ]

inputFiles :: Parser (IO [DataSource (SafeT IO)])
inputFiles =
    concatThem <$> some (argument (parse <$> str) (metavar "FILE" <> help "TREC input file"))
  where
    concatThem :: [IO [DataSource (SafeT IO)]] -> IO [DataSource (SafeT IO)]
    concatThem = fmap concat . sequence

    parse :: String -> IO [DataSource (SafeT IO)]
    parse ('@':rest) = map parse' . lines <$> readFile rest
    parse fname      = return [parse' fname]
    parse'           = fromMaybe (error "unknown input file type") . parseDataSource dsrcs . T.pack

    dsrcs = localFile


facEntities :: [DataSource (SafeT IO)]
            -> Producer (DocumentInfo, [Fac.EntityId]) (SafeT IO) ()
facEntities dsrcs =
    mapM_ (\dsrc -> Fac.parseDocuments (P.T.decodeUtf8 $ runDataSource dsrc)
                    >-> P.P.map (\d -> ( DocInfo (Fac.docArchive d)
                                                 (DocName $ Utf8.fromText
                                                            -- Drop timestamp prefix
                                                          $ T.drop 1 $ T.dropWhile (/= '-') $ Fac.docId d)
                                                 (DocLength $ length $ Fac.docAnnotations d)
                                       , map Fac.annEntity (Fac.docAnnotations d)))
          ) dsrcs

buildIndex :: DiskIndex -> IO [DataSource (SafeT IO)] -> IO ()
buildIndex output readDocLocs = do
    dsrcs <- readDocLocs

    let chunkIndexes :: Producer (FragmentIndex TermFrequency) (SafeT IO) ()
        chunkIndexes =
                foldChunks 50000 (Foldl.generalize $ indexPostings id)
             $  facEntities dsrcs
            >-> cat'                                              @(DocumentInfo, [EntityId])
            >-> P.P.map (second $ \postings -> foldTokens Foldl.mconcat $ zip postings (repeat $ TermFreq 1))
            >-> cat'                                              @( DocumentInfo
                                                                   , M.Map EntityId TermFrequency
                                                                   )

    chunks <- runSafeT $ P.P.toListM $ for (chunkIndexes >-> zipWithList [0..]) $ \(n, (docs, termFreqs, corpusStats)) -> do
        liftIO $ print (n :: Integer, M.size docs)
        root <- liftIO $ createTempDirectory "." "index"
        let indexPaths = diskIndexPaths root

        let docsPath :: BTree.BTreePath DocumentName (DocumentInfo, M.Map EntityId TermFrequency)
            docsPath = diskDocuments indexPaths
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size docs) docsPath (each $ M.toAscList docs)

        let termFreqsPath :: BTree.BTreePath EntityId TermStats
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

indexPostings :: forall p. ()
              => (p -> TermFrequency)
              -> Fold (DocumentInfo, M.Map EntityId p)
                      (FragmentIndex p)
indexPostings getTermFreq =
    (,,)
      <$> docMeta
      <*> lmap snd termFreqs
      <*> lmap fst foldCorpusStats
  where
    docMeta :: Fold (DocumentInfo, M.Map EntityId p) (M.Map DocumentName (DocumentInfo, M.Map EntityId p))
    docMeta  = lmap (\(docInfo, terms) -> (docName docInfo, (docInfo, terms))) Foldl.map

    termFreqs :: Fold (M.Map EntityId p) (M.Map EntityId TermStats)
    termFreqs = lmap M.assocs
                $ Foldl.handles traverse
                $ lmap (\(term, ps) -> (term, TermStats (getTermFreq ps) (DocumentFrequency 1)))
                $ Foldl.map

foldCorpusStats :: Foldl.Fold DocumentInfo CorpusStats
foldCorpusStats =
    CorpusStats
      <$> lmap (fromEnum . docLength) Foldl.sum
      <*> Foldl.length

