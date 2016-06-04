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
import Data.Monoid
import Data.Profunctor
import Data.Tuple
import Data.Char
import Numeric.Log

import Data.Binary
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Control.Foldl as Foldl

import           Pipes
import           Pipes.Safe
import qualified Pipes.Prelude as P.P
import qualified Pipes.ByteString as P.BS

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import Utils
import Types
import Term
import Tokenise
import DataSource
import TopK
import qualified SimplIR.TrecStreaming as Trec
import RetrievalModels.QueryLikelihood

type QueryId = String

scoreMode :: Parser (IO ())
scoreMode =
    score
      <$> option str (metavar "QUERY_ID" <> long "qid" <> short 'i' <> value "1")
      <*> queryTerms
      <*> option auto (metavar "N" <> long "count" <> short 'n' <> value 10)
      <*> some (argument (LocalFile <$> str) (metavar "FILE" <> help "TREC input file"))
  where
    queryTerms :: Parser (M.Map Term Int)
    queryTerms = M.fromListWith (+) . flip zip (repeat 1)
                 <$> some (option (Term.fromString <$> str)
                                  (metavar "TERMS" <> long "query" <> short 'q'))

corpusStatsMode :: Parser (IO ())
corpusStatsMode =
    corpusStats
      <$> queryTerms
      <*> some (argument (LocalFile <$> str) (metavar "FILE" <> help "TREC input file"))
  where
    queryTerms :: Parser (S.Set Term)
    queryTerms = S.fromList
                 <$> some (option (Term.fromString <$> str)
                                  (metavar "TERMS" <> long "query" <> short 'q'))

modes :: Parser (IO ())
modes = subparser
    $  command "score" (info scoreMode fullDesc)
    <> command "corpus-stats" (info corpusStatsMode fullDesc)

compression = Just Lzma

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

corpusStats :: S.Set Term -> [DataLocation] -> IO ()
corpusStats queryTerms docs = do
    runSafeT $ do
        idx@(termFreqs, collLength) <-
                foldProducer (Foldl.generalize indexPostings)
             $  streamingDocuments docs >-> normalizationPipeline
            >-> cat'                                @(DocumentName, [(Term, Position)])
            >-> P.P.map (second $ map fst)
            >-> cat'                                @(DocumentName, [Term])
            >-> P.P.map (second $ filter ((`S.member` queryTerms)))

        liftIO $ putStrLn $ "Indexed "++show collLength++" documents with "++show (M.size termFreqs)++" terms"
        liftIO $ BS.L.writeFile "background" $ encode idx

type CollectionLength = Int
type CorpusStats = ( M.Map Term TermFrequency
                   , CollectionLength
                   )

indexPostings :: Foldl.Fold (DocumentName, [Term]) CorpusStats
indexPostings =
    (,)
      <$> lmap snd termFreqs
      <*> lmap (const 1) Foldl.sum
  where
    termFreqs :: Foldl.Fold [Term] (M.Map Term TermFrequency)
    termFreqs =
          Foldl.handles traverse
        $ lmap (\term -> M.singleton term (TermFreq 1))
        $ mconcatMaps

score :: QueryId -> M.Map Term Int -> Int -> [DataLocation] -> IO ()
score qid queryTerms resultCount docs = do
    -- background statistics
    (termFreqs, collLength) <- decode <$> BS.L.readFile "background"
        :: IO CorpusStats
    let getTermFreq term = maybe mempty id $ M.lookup term termFreqs
        smoothing = Dirichlet 2500 ((\n -> (n + 0.5) / (realToFrac collLength + 1)) . getTermFrequency . getTermFreq)

    let scoreTerms :: [(Term, Position)] -> Score
        scoreTerms terms =
            let docLength = DocLength $ length terms
                terms' = map (\(term,_) -> (term, 1)) terms
            in queryLikelihood smoothing (M.assocs queryTerms) docLength terms'

    runSafeT $ do
        results <-
                foldProducer (Foldl.generalize $ topK resultCount)
             $  streamingDocuments docs
            >-> normalizationPipeline
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> P.P.filter (any (`M.member` queryTerms) . map fst . snd)
            >-> P.P.map (swap . second scoreTerms)
            >-> cat'                                          @(Score, DocumentName)

        let toRunFile rank (Exp score, DocName docName) = unwords
                [ qid, "Q0", Utf8.toString docName, show rank, show score, "simplir" ]
        liftIO $ putStrLn $ unlines $ zipWith toRunFile [1..] results

type ArchiveName = T.Text

streamingDocuments :: [DataLocation]
                   -> Producer (ArchiveName, Trec.StreamItem) (SafeT IO) ()
streamingDocuments dsrcs =
    mapM_ (\src -> do
                bs <- P.BS.toLazyM (decompress compression $ produce src)
                mapM_ (yield . (getFileName src,)) (Trec.readItems $ BS.L.toStrict bs)
          ) dsrcs

normalizationPipeline
    :: Monad m
    => Pipe (ArchiveName, Trec.StreamItem)
            (DocumentName, [(Term, Position)]) m ()
normalizationPipeline =
          P.P.mapFoldable
              (\(archive, d) -> do
                    body <- Trec.body d
                    visible <- Trec.cleanVisible body
                    let docName =
                            DocName $ Utf8.fromText $ archive <> Trec.getDocumentId (Trec.documentId d)
                    return (docName, visible))
      >-> cat'                                          @(DocumentName, T.Text)
      >-> P.P.map (fmap tokeniseWithPositions)
      >-> cat'                                          @(DocumentName, [(T.Text, Position)])
      >-> P.P.map (fmap normTerms)
      >-> cat'                                          @(DocumentName, [(Term, Position)])
  where
    normTerms :: [(T.Text, p)] -> [(Term, p)]
    normTerms = map (first Term.fromText) . filterTerms . caseNorm
      where
        filterTerms = filter ((>2) . T.length . fst)
        caseNorm = map (first $ T.filter isAlpha . T.toCaseFold)
