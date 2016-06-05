{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveGeneric #-}

import Control.Monad.State.Strict hiding ((>=>))
import Data.Bifunctor
import Data.Foldable (fold)
import Data.Maybe
import Data.Monoid
import Data.Profunctor
import Data.Tuple
import Data.Char
import GHC.Generics
import System.IO

import Data.Binary
import qualified Data.Aeson as Aeson
import Data.Aeson ((.=))
import Numeric.Log hiding (sum)
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
type StatsFile = FilePath

scoreMode :: Parser (IO ())
scoreMode =
    score
      <$> optQueryFile
      <*> option auto (metavar "N" <> long "count" <> short 'n' <> value 10)
      <*> option str (metavar "FILE" <> long "stats" <> short 's'
                      <> help "background corpus statistics file")
      <*> some (argument (LocalFile <$> str) (metavar "FILE" <> help "TREC input file"))

corpusStatsMode :: Parser (IO ())
corpusStatsMode =
    corpusStats
      <$> optQueryFile
      <*> option str (metavar "FILE" <> long "output" <> short 'o'
                      <> help "output file path")
      <*> some (argument (LocalFile <$> str) (metavar "FILE" <> help "TREC input file"))

dumpDocumentMode :: Parser (IO ())
dumpDocumentMode =
    dumpDocument
      <$> optQueryFile
      <*> option (LocalFile <$> str)
                 (metavar "FILE" <> long "archive" <> short 'a' <> help "archive file")
      <*> S.fromList `fmap` some (argument (Trec.DocumentId . T.pack <$> str) $ metavar "DOC_ID" <> help "document id to export")

modes :: Parser (IO ())
modes = subparser
    $  command "score" (info scoreMode fullDesc)
    <> command "corpus-stats" (info corpusStatsMode fullDesc)
    <> command "dump-doc" (info dumpDocumentMode fullDesc)

type QueryFile = FilePath

optQueryFile :: Parser QueryFile
optQueryFile =
    option str (metavar "FILE" <> long "query" <> short 'q' <> help "query file")

readQueries :: QueryFile -> IO (M.Map QueryId [Term])
readQueries fname = do
    queries <- M.unions . mapMaybe parse . lines <$> readFile fname
    let allTerms = foldMap S.fromList queries
    hPutStrLn stderr $ show (M.size queries)++" queries with "++show (S.size allTerms)++" unique terms"
    return queries
  where
    parse "" = Nothing
    parse line
      | (qid, body) <- span (/= '\t') line
      = Just $ M.singleton qid (map Term.fromString $ words body)

compression = Just Lzma

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

corpusStats :: QueryFile -> StatsFile -> [DataLocation] -> IO ()
corpusStats queryFile outputFile docs = do
    queries <- readQueries queryFile
    let queryTerms = foldMap S.fromList queries
    runSafeT $ do
        stats <-
                foldProducer (Foldl.generalize indexPostings)
             $  streamingDocuments docs >-> normalizationPipeline
            >-> cat'                                @(DocumentName, [(Term, Position)])
            >-> P.P.map (second $ map fst)
            >-> cat'                                @(DocumentName, [Term])
            >-> P.P.map (second $ filter ((`S.member` queryTerms)))

        liftIO $ putStrLn $ "Indexed "++show (corpusCollectionLength stats)
                          ++" documents with "++show (M.size $ corpusTermFreqs stats)++" terms"
        liftIO $ BS.L.writeFile outputFile $ encode stats
        liftIO $ putStrLn $ "Saw "++show (fold $ corpusTermFreqs stats)++" term occurrences"

type CollectionLength = Int
data CorpusStats = CorpusStats { corpusTermFreqs :: !(M.Map Term TermFrequency)
                               , corpusCollectionLength :: !CollectionLength
                               }
                 deriving (Generic)

instance Binary CorpusStats
instance Monoid CorpusStats where
    mempty = CorpusStats mempty 0
    a `mappend` b =
        CorpusStats { corpusTermFreqs = corpusTermFreqs a <> corpusTermFreqs b
                    , corpusCollectionLength = corpusCollectionLength a + corpusCollectionLength b
                    }

indexPostings :: Foldl.Fold (DocumentName, [Term]) CorpusStats
indexPostings =
    CorpusStats
      <$> lmap snd termFreqs
      <*> lmap (const 1) Foldl.sum
  where
    termFreqs :: Foldl.Fold [Term] (M.Map Term TermFrequency)
    termFreqs =
          Foldl.handles traverse
        $ lmap (\term -> M.singleton term (TermFreq 1))
        $ mconcatMaps

score :: QueryFile -> Int -> FilePath -> [DataLocation] -> IO ()
score queryFile resultCount statsFile docs = do
    queries <- readQueries queryFile

    -- load background statistics
    CorpusStats termFreqs collLength <- decode <$> BS.L.readFile statsFile
        :: IO CorpusStats
    let getTermFreq term = maybe mempty id $ M.lookup term termFreqs
        smoothing = Dirichlet 2500 ((\n -> (n + 0.5) / (realToFrac collLength + 1)) . getTermFrequency . getTermFreq)


    let queriesFold :: Foldl.Fold (DocumentName, [Term])
                                  (M.Map QueryId [(Score, DocumentName)])
        queriesFold = traverse queryFold queries

        queryFold :: [Term] -> Foldl.Fold (DocumentName, [Term]) [(Score, DocumentName)]
        queryFold queryTerms =
              Foldl.handles (Foldl.filtered (\(_, docTerms) -> any (`M.member` queryTerms') docTerms))
            $ lmap (swap . second scoreTerms)
            $ topK resultCount
          where
            queryTerms' :: M.Map Term Int
            queryTerms' = M.fromListWith (+) $ zip queryTerms (repeat 1)

            scoreTerms :: [Term] -> Score
            scoreTerms docTerms =
                let docLength = DocLength $ length docTerms
                    docTerms' = zip docTerms (repeat 1)
                in queryLikelihood smoothing (M.assocs queryTerms') docLength docTerms'

    runSafeT $ do
        results <-
                foldProducer (Foldl.generalize queriesFold)
             $  streamingDocuments docs
            >-> normalizationPipeline
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> P.P.map (second $ map fst)
            >-> cat'                                          @(DocumentName, [Term])

        liftIO $ putStrLn $ unlines
            [ unwords [ qid, "Q0", Utf8.toString docName, show rank, show score, "simplir" ]
            | (qid, scores) <- M.toList results
            , (rank, (Exp score, DocName docName)) <- zip [1..] scores
            ]
        return ()

dumpDocument :: QueryFile -> DataLocation -> S.Set Trec.DocumentId -> IO ()
dumpDocument queryFile archive docIds = do
    queries <- readQueries queryFile
    let allQueryTerms = foldMap S.fromList queries
    runSafeT $ runEffect $ do
            (>-> P.BS.stdout)
         $  toJsonArray
         $  streamingDocuments [archive]
        >-> P.P.filter (\(_,doc) -> Trec.documentId doc `S.member` docIds)
        >-> normalizationPipeline
        >-> cat'                                            @(DocumentName, [(Term, Position)])
        >-> P.P.map (fmap $ filter (\(term,_) -> term `S.member` allQueryTerms))
        >-> cat'                                            @(DocumentName, [(Term, Position)])
        >-> P.P.map ( \(docName, terms) ->
                        let positionalPostings :: [(Term, Position)] -> M.Map Term [Position]
                            positionalPostings terms =         -- turn [(term, pos)] into [(term, [pos])]
                                M.unionsWith (++)
                                  [ M.singleton term [pos]
                                  | (term, pos) <- terms ]
                        in Aeson.object
                           [ "docname".= Utf8.toText (getDocName docName),
                             "termpostings".=
                             [ Aeson.object [
                                   "term".= term,
                                   "positions".=
                                     [ Aeson.object
                                       [ "tokenpos".= tokenN pos,
                                         "charpos".= Aeson.object
                                             [ "begin".= begin (charOffset pos),
                                               "end".= end (charOffset pos)
                                             ]
                                       ]  | pos <- poss ]
                                   ]
                             | (term, poss) <- M.toList $ positionalPostings terms ]
                           ])
    return ()

type ArchiveName = T.Text

streamingDocuments :: [DataLocation]
                   -> Producer (ArchiveName, Trec.StreamItem) (SafeT IO) ()
streamingDocuments dsrcs =
    mapM_ (\src -> do
                liftIO $ hPutStrLn stderr $ show src
                bs <- P.BS.toLazyM (decompress compression $ produce src)
                mapM_ (yield . (getFilePath src,)) (Trec.readItems $ BS.L.toStrict bs)
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
                            DocName $ Utf8.fromText $ archive <> ":" <> Trec.getDocumentId (Trec.documentId d)
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
