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
import Data.Foldable (fold, toList)
import Data.Maybe
import Data.Monoid
import Data.Profunctor
import Data.Char
import GHC.Generics
import System.IO
import System.FilePath
import System.Directory (createDirectoryIfMissing)

import Data.Binary
import qualified Data.Aeson as Aeson
import Data.Aeson ((.=))
import Numeric.Log hiding (sum)
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.HashSet as HS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Control.Foldl as Foldl
import qualified Data.Vector as V

import           Pipes
import           Pipes.Safe
import qualified Pipes.Text.Encoding as P.T
import qualified Pipes.ByteString as P.BS
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
import qualified SimplIR.DiskIndex.Build as BuildIdx
import qualified SimplIR.DiskIndex.Document as DocIdx
import SimplIR.TopK
import qualified SimplIR.TREC as Trec
import qualified SimplIR.TrecStreaming as Kba
import SimplIR.RetrievalModels.QueryLikelihood
import SimplIR.RetrievalModels.CorpusStats
import qualified SimplIR.HTML.Clean as HTML.Clean

type QueryId = T.Text
type StatsFile = FilePath

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

type DocumentSource = [DataSource] -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()

optDocumentSource :: Parser DocumentSource
optDocumentSource =
    option (parse <$> str) (help "document type (kba or robust)" <> value kbaDocuments
                           <> short 'f' <> long "format")
  where
    parse "kba"         = kbaDocuments
    parse "robust"      = trecDocuments
    parse "robust-html" = trecHtmlDocuments
    parse _             = fail "unknown document source type"

indexMode :: Parser (IO ())
indexMode =
    buildIndex
      <$> optDocumentSource
      <*> inputFiles

modes :: Parser (IO ())
modes = subparser
    $ command "index" (info indexMode fullDesc)

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode


buildIndex :: DocumentSource -> IO [DataSource] -> IO ()
buildIndex docSource readDocLocs = do
    docs <- readDocLocs
    let --foldCorpusStats = Foldl.generalize documentTermStats
        indexFold = (,) <$> BuildIdx.buildIndex 100000 "index" <*> pure ()
        toIndexDoc ((archiveName, docName), text) =
            ((archiveName, docName), tokenise text)
    (idx, corpusStats) <- runSafeT $ foldProducer indexFold
        $ docSource docs
       >-> normalizationPipeline
       >-> P.P.map (second M.fromList)
    return ()

type ArchiveName = T.Text

trecSource :: [DataSource]
           -> Producer ((ArchiveName, DocumentName), Trec.Document) (SafeT IO) ()
trecSource dsrcs =
    mapM_ (\dsrc -> Trec.trecDocuments' (P.T.decodeIso8859_1 $ dataSource dsrc)
                    >-> P.P.map (\d -> ( ( getFileName $ dsrcLocation dsrc
                                         , DocName $ Utf8.fromText $ Trec.docNo d)
                                       , d))
                    >-> P.P.chain (liftIO . print . fst)
          ) dsrcs

trecDocuments :: [DataSource]
              -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
trecDocuments dsrcs =
    trecSource dsrcs >-> P.P.map (second Trec.docText)

trecHtmlDocuments :: [DataSource]
                  -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
trecHtmlDocuments dsrcs =
    trecSource dsrcs >-> P.P.map (second $ scrapeHtml  . Trec.docHtml)
  where
    scrapeHtml = TL.toStrict . HTML.Clean.docBody . HTML.Clean.cleanTokens

kbaDocuments :: [DataSource]
             -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
kbaDocuments dsrcs =
    mapM_ (\src -> do
                liftIO $ hPutStrLn stderr $ show src
                bs <- P.BS.toLazyM (dataSource src)
                mapM_ (yield . (getFilePath $ dsrcLocation src,)) (Kba.readItems $ BS.L.toStrict bs)
          ) dsrcs
    >-> P.P.mapFoldable
              (\(archive, d) -> do
                    body <- Kba.body d
                    visible <- Kba.cleanVisible body
                    let docName =
                            DocName $ Utf8.fromText $ Kba.getDocumentId (Kba.documentId d)
                    return ((archive, docName), visible))

data DocumentInfo = DocInfo { docArchive :: ArchiveName
                            , docName    :: DocumentName
                            , docLength  :: DocumentLength
                            }
                  deriving (Generic, Eq, Ord, Show)
instance Binary DocumentInfo

normalizationPipeline
    :: MonadIO m
    => Pipe ((ArchiveName, DocumentName), T.Text)
            (DocumentInfo, [(Term, Position)]) m ()
normalizationPipeline =
          cat'                                          @((ArchiveName, DocumentName), T.Text)
      >-> P.P.map (fmap $ T.map killPunctuation)
      >-> P.P.map (fmap tokeniseWithPositions)
      >-> cat'                                          @((ArchiveName, DocumentName), [(T.Text, Position)])
      >-> P.P.map (\((archive, docName), terms) ->
                      let docLen = DocLength $ length $ filter (not . T.all (not . isAlphaNum) . fst) terms
                      in (DocInfo archive docName docLen, terms))
      >-> cat'                                          @( DocumentInfo, [(T.Text, Position)])
      >-> P.P.map (fmap normTerms)
      >-> cat'                                          @( DocumentInfo, [(Term, Position)])
      >-> P.P.chain (liftIO . print . fst)
  where
    normTerms :: [(T.Text, p)] -> [(Term, p)]
    normTerms = map (first Term.fromText) . filterTerms . caseNorm
      where
        filterTerms = filter ((>2) . T.length . fst)
        caseNorm = map (first $ T.filter isAlpha . T.toCaseFold)

    killPunctuation c
      | c `HS.member` chars = ' '
      | otherwise           = c
      where chars = HS.fromList "\t\n\r;\"&/:!#?$%()@^*+-,=><[]{}|`~_`"
