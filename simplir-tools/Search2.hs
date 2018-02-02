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
import Control.Concurrent.Async
import Data.List.Split
import Data.Bifunctor
import Data.Maybe
import Data.Monoid
import Data.Char
import qualified Control.Foldl as Foldl
import GHC.Generics
import System.IO
import Control.DeepSeq

import Codec.Serialise
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.HashSet as HS
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL

import           Pipes
import           Pipes.Safe
import qualified Pipes.Text.Encoding as P.T
import qualified Pipes.ByteString as P.BS
import qualified Pipes.Prelude as P.P

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import SimplIR.Utils
import SimplIR.Types
import SimplIR.Term as Term
import SimplIR.Tokenise
import SimplIR.DataSource
import SimplIR.DataSource.Compression
import qualified SimplIR.KyotoIndex as KI
import qualified SimplIR.TREC as Trec
import qualified SimplIR.TrecStreaming as Kba
import qualified SimplIR.HTML.Clean as HTML.Clean

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

type DocumentSource = [DataSource (SafeT IO)]
                    -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()

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

buildIndex :: DocumentSource -> IO [DataSource (SafeT IO)] -> IO ()
buildIndex docSource readDocLocs = do
    docs <- readDocLocs
    indexPath <- KI.create "index"
    KI.withIndex indexPath $ \idx ->
        mapConcurrently_ (run idx) (chunksOf 10 docs)
  where
    run idx docs = runSafeT $ do
        let --foldCorpusStats = Foldl.generalize documentTermStats
            indexFold = (,) <$> KI.addDocuments idx <*> pure ()
        (idx, corpusStats) <- foldProducer indexFold
            $ foldChunks 1000 (Foldl.generalize Foldl.vector)
            $ docSource docs
          >-> normalizationPipeline
          >-> P.P.map (second $ M.fromList . map (second $ \x -> ((), x)))
        return ()

type ArchiveName = T.Text

trecSource :: [DataSource (SafeT IO)]
           -> Producer ((ArchiveName, DocumentName), Trec.Document) (SafeT IO) ()
trecSource dsrcs =
    mapM_ (\dsrc -> Trec.trecDocuments' (P.T.decodeIso8859_1 $ decompressed $ runDataSource dsrc)
                    >-> P.P.map (\d -> ( ( T.pack $ dataSourceFileName dsrc
                                         , DocName $ Utf8.fromText $ Trec.docNo d)
                                       , d))
                    >-> P.P.chain (liftIO . print . fst)
          ) dsrcs


trecDocuments :: [DataSource (SafeT IO)]
              -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
trecDocuments dsrcs =
    trecSource dsrcs >-> P.P.map (second Trec.docBody)

trecHtmlDocuments :: [DataSource (SafeT IO)]
                  -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
trecHtmlDocuments dsrcs =
    trecSource dsrcs
      >-> P.P.map (second $ maybeScrape . Trec.docBody)
  where
    maybeScrape s
      | isHtml s  = scrapeHtml s
      | otherwise = s

    scrapeHtml = TL.toStrict . HTML.Clean.docBody . HTML.Clean.clean

    isHtml s =
        let beginning = T.toCaseFold $ T.take 200 s
        in "<html" `T.isInfixOf` beginning
           || "<!doctype" `T.isInfixOf` beginning

kbaDocuments :: [DataSource (SafeT IO)]
             -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
kbaDocuments dsrcs =
    mapM_ (\src -> do
                liftIO $ hPutStrLn stderr $ show src
                bs <- lift $ P.BS.toLazyM (runDataSource src)
                mapM_ (yield . (T.pack $ dataSourceFileName src,)) (Kba.readItems $ BS.L.toStrict bs)
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
instance Serialise DocumentInfo
instance NFData DocumentInfo

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
