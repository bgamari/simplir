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
import qualified Data.Text.IO as T.IO
import qualified Control.Foldl as Foldl

import           Pipes
import           Pipes.Safe
import qualified Pipes.ByteString as P.BS
import qualified Pipes.Prelude as P.P

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import SimplIR.Utils
import Control.Foldl.Map
import SimplIR.Types
import SimplIR.Term as Term
import SimplIR.Tokenise
import SimplIR.DataSource
import SimplIR.BinaryFile as BinaryFile
import qualified BTree.File as BTree
import SimplIR.TopK
import qualified SimplIR.TrecStreaming as Kba
import SimplIR.RetrievalModels.QueryLikelihood
import qualified Types as Fac
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac

type QueryId = T.Text

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

streamMode :: Parser (IO ())
streamMode =
    scoreStreaming
      <$> optQueryFile
      <*> option (Fac.diskIndexPaths <$> str) (metavar "DIR" <> long "fac-index" <> short 'f')
      <*> option auto (metavar "N" <> long "count" <> short 'n' <> value 10)
      <*> option (corpusStatsPaths <$> str)
                 (metavar "PATH" <> long "stats" <> short 's'
                 <> help "background corpus statistics index")
      <*> option str (metavar "PATH" <> long "output" <> short 'o'
                      <> help "output file name")
      <*> pure kbaDocuments
      <*> inputFiles

corpusStatsMode :: Parser (IO ())
corpusStatsMode =
    corpusStats
      <$> optQueryFile
      <*> option (corpusStatsPaths <$> str) (metavar "FILE" <> long "output" <> short 'o'
                                             <> help "output file path")
      <*> pure kbaDocuments
      <*> inputFiles


modes :: Parser (IO ())
modes = subparser
    $  command "score" (info streamMode fullDesc)
    <> command "corpus-stats" (info corpusStatsMode fullDesc)

type QueryFile = FilePath

optQueryFile :: Parser QueryFile
optQueryFile =
    option str (metavar "FILE" <> long "query" <> short 'q' <> help "query file")

data Query = Query { queryTerms :: [Term]
                   , queryEntities :: [Fac.EntityId]
                   }

readQueries :: QueryFile -> IO (M.Map QueryId Query)
readQueries fname = do
    queries <- M.unions . mapMaybe parse . T.lines <$> T.IO.readFile fname
    let allTerms = foldMap (S.fromList . queryTerms) queries
    hPutStrLn stderr $ show (M.size queries)++" queries with "++show (S.size allTerms)++" unique terms"
    return queries
  where
    parse line
      | T.null line
      = Nothing
      | [qid, terms, _names, entityIds] <- T.split (== '\t') line
      = Just $ M.singleton qid
        $ Query { queryTerms    = map Term.fromText $ T.words terms
                , queryEntities = map Fac.EntityId $ T.words entityIds
                }
      | otherwise
      = error $ "error parsing line: "++show line

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

corpusStats :: QueryFile -> CorpusStatsPaths -> DocumentSource -> IO [DataSource] -> IO ()
corpusStats queryFile output docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    createDirectoryIfMissing True (corpusStatsRoot output)
    let allQueryTerms = foldMap (S.fromList . queryTerms) queries
    runSafeT $ do
        (corpusStats, termStats) <-
                foldProducer (Foldl.generalize foldCorpusStats)
             $  docSource docs >-> normalizationPipeline
            >-> cat'                                @(DocumentInfo, [(Term, Position)])
            >-> P.P.map (second $ filter (`S.member` allQueryTerms) . map fst)
            >-> cat'                                @(DocumentInfo, [Term])

        liftIO $ putStrLn $ "Indexed "++show (corpusCollectionLength corpusStats)
                          ++" documents with "++show (corpusCollectionSize corpusStats)++" terms"
        liftIO $ BinaryFile.write (diskCorpusStats output) corpusStats
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size termStats) (diskTermStats output) (each $ M.assocs termStats)

data DocumentInfo = DocInfo { docArchive :: ArchiveName
                            , docName    :: DocumentName
                            , docLength  :: DocumentLength
                            }
                  deriving (Generic, Eq, Ord, Show)
instance Binary DocumentInfo

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

data TermStats = TermStats !TermFrequency !DocumentFrequency
               deriving (Generic)
instance Binary TermStats
instance Monoid TermStats where
    mempty = TermStats mempty mempty
    TermStats a b `mappend` TermStats c d = TermStats (a `mappend` c) (b `mappend` d)

data CorpusStatsPaths = CorpusStatsPaths { corpusStatsRoot :: FilePath
                                         , diskCorpusStats :: BinaryFile CorpusStats
                                         , diskTermStats   :: BTree.BTreePath Term TermStats
                                         }

corpusStatsPaths :: FilePath -> CorpusStatsPaths
corpusStatsPaths root =
    CorpusStatsPaths { corpusStatsRoot = root
                     , diskCorpusStats = BinaryFile $ root </> "corpus-stats"
                     , diskTermStats = BTree.BTreePath $ root </> "term-freqs"
                     }

foldCorpusStats :: Foldl.Fold (DocumentInfo, [Term]) (CorpusStats, M.Map Term TermStats)
foldCorpusStats =
    (,)
      <$> lmap fst foldCorpusStats'
      <*> lmap snd foldTermStats

foldCorpusStats' :: Foldl.Fold DocumentInfo CorpusStats
foldCorpusStats' =
    CorpusStats
      <$> lmap (fromEnum . docLength) Foldl.sum
      <*> Foldl.length

foldTermStats :: Foldl.Fold [Term] (M.Map Term TermStats)
foldTermStats =
    M.mergeWithKey (\_ x y -> Just (TermStats x y)) (fmap (\x -> TermStats x mempty)) (fmap (\y -> TermStats mempty y))
      <$> termFreqs
      <*> docFreqs
  where
    docFreqs :: Foldl.Fold [Term] (M.Map Term DocumentFrequency)
    docFreqs = lmap (\terms -> M.fromList $ zip terms (repeat $ DocumentFrequency 1)) mconcatMaps
    termFreqs :: Foldl.Fold [Term] (M.Map Term TermFrequency)
    termFreqs =
          Foldl.handles traverse
        $ lmap (\term -> M.singleton term (TermFreq 1))
        $ mconcatMaps

data ScoredDocument = ScoredDocument { scoredRankScore     :: Score
                                     , scoredDocumentInfo  :: DocumentInfo
                                     , scoredTermPositions :: M.Map Term [Position]
                                     , scoredTermScore     :: Score
                                     , scoredEntityFreqs   :: M.Map Fac.EntityId TermFrequency
                                     , scoredEntityScore   :: Score
                                     }
                    deriving (Show, Ord, Eq)

queryFold :: Smoothing Term
          -> Smoothing Fac.EntityId
          -> Int                        -- ^ how many results should we collect?
          -> (Log Double, Log Double)   -- ^ weights for document subscores
          -> Query
          -> Foldl.Fold (DocumentInfo, M.Map Term [Position], (DocumentLength, M.Map Fac.EntityId TermFrequency))
                        [ScoredDocument]
queryFold termSmoothing entitySmoothing resultCount (weightTerm, weightEntity) query =
      Foldl.handles (Foldl.filtered (\(_, docTerms, _) -> not $ S.null $ M.keysSet queryTerms' `S.intersection` M.keysSet docTerms))
    $ lmap scoreQuery
    $ topK resultCount
  where
    queryTerms' :: M.Map Term Int   -- query term frequency
    queryTerms' = M.fromListWith (+) $ zip (queryTerms query) (repeat 1)

    queryEntities' :: M.Map Fac.EntityId Int   -- query entity frequency
    queryEntities' = M.fromListWith (+) $ zip (queryEntities query) (repeat 1)

    scoreTerms :: (DocumentInfo, M.Map Term [Position])
                -> Score
    scoreTerms (info, docTerms) =
        queryLikelihood termSmoothing (M.assocs queryTerms')
                        (docLength info)
                        (M.toList $ fmap length docTerms)

    scoreEntities :: (DocumentLength, M.Map Fac.EntityId TermFrequency)
                  -> Score
    scoreEntities (entityDocLen, entityFreqs) =
        queryLikelihood entitySmoothing (M.assocs queryEntities')
                        entityDocLen
                        (M.toList $ fmap fromEnum entityFreqs)

    scoreQuery :: (DocumentInfo, M.Map Term [Position], (DocumentLength, M.Map Fac.EntityId TermFrequency))
               -> ScoredDocument
    scoreQuery (info, docTermPositions, (entityDocLen, entityFreqs)) =
        let termScore  = scoreTerms (info, docTermPositions)
            entityScore = scoreEntities (entityDocLen, entityFreqs)
            score = weightTerm * termScore
                    + weightEntity * entityScore
        in ScoredDocument { scoredRankScore = score
                          , scoredDocumentInfo = info
                          , scoredTermPositions = docTermPositions
                          , scoredTermScore = termScore
                          , scoredEntityFreqs = entityFreqs
                          , scoredEntityScore = entityScore
                          }

scoreStreaming :: QueryFile -> Fac.DiskIndex -> Int -> CorpusStatsPaths -> FilePath -> DocumentSource -> IO [DataSource] -> IO ()
scoreStreaming queryFile facIndexPath resultCount background outputRoot docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    let allQueryTerms = foldMap (S.fromList . queryTerms) queries

    -- load FAC annotations
    facIndex <- BTree.open $ Fac.diskDocuments facIndexPath
    facEntityIdStats <- BTree.open $ Fac.diskTermStats facIndexPath
    facCorpusStats <- BinaryFile.read $ Fac.diskCorpusStats facIndexPath

    -- load background statistics
    CorpusStats collLength _collSize <- BinaryFile.read (diskCorpusStats background)
    termFreqs <- BTree.open (diskTermStats background)
    let termSmoothing :: Smoothing Term
        termSmoothing =
            Dirichlet 2500 $ \term ->
                case BTree.lookup termFreqs term of
                  Just (TermStats tf _) -> getTermFrequency tf / realToFrac collLength
                  Nothing               -> 0.5 / realToFrac collLength

        entitySmoothing :: Smoothing Fac.EntityId
        entitySmoothing =
            Dirichlet 250 $ \entity ->
                let collLength = Fac.corpusCollectionLength facCorpusStats
                in case BTree.lookup facEntityIdStats entity of
                     Just (Fac.TermStats tf _) -> getTermFrequency tf / realToFrac collLength
                     Nothing                   -> 0.05 / realToFrac collLength

    let queriesFold :: Foldl.Fold (DocumentInfo, M.Map Term [Position], (DocumentLength, M.Map Fac.EntityId TermFrequency))
                                  (M.Map QueryId [ScoredDocument])
        queriesFold = traverse (queryFold termSmoothing entitySmoothing resultCount (0.5, 0.5))
                               queries

    runSafeT $ do
        results <-
                foldProducer (Foldl.generalize queriesFold)
             $  docSource docs
            >-> normalizationPipeline
            >-> cat'                         @(DocumentInfo, [(Term, Position)])
            >-> P.P.map (second $ filter ((`S.member` allQueryTerms) . fst))
            >-> P.P.map (second $ M.fromListWith (++) . map (second (:[])))
            >-> cat'                         @(DocumentInfo, M.Map Term [Position])
            >-> P.P.map (\(docInfo, termPostings) ->
                            let (facDocLen, entityIdPostings) =
                                    maybe (DocLength 0, M.empty) (first Fac.docLength)
                                    $ BTree.lookup facIndex (docName docInfo)
                            in (docInfo, termPostings, (facDocLen, entityIdPostings))
                        )
            >-> cat'                         @( DocumentInfo
                                              , M.Map Term [Position]
                                              , (DocumentLength, M.Map Fac.EntityId TermFrequency)
                                              )

        liftIO $ writeFile (outputRoot<.>"run") $ unlines
            [ unwords [ T.unpack qid
                      , T.unpack docArchive
                      , Utf8.toString $ getDocName docName
                      , show rank
                      , show scoredRankScore
                      , "simplir" ]
            | (qid, scores) <- M.toList results
            , (rank, ScoredDocument {..}) <- zip [1 :: Integer ..] scores
            , let DocInfo {..} = scoredDocumentInfo
            ]

        liftIO $ BS.L.writeFile (outputRoot<.>"json") $ Aeson.encode
            [ Aeson.object
              [ "query_id" .= qid
              , "results"  .=
                [ Aeson.object
                  [ "doc_name" .= getDocName docName
                  , "length"   .= docLength
                  , "archive"  .= docArchive
                  , "score"    .= ln scoredRankScore
                  , "postings" .= [
                        Aeson.object
                          [ "term" .= term
                          , "positions" .= [
                                Aeson.object
                                  [ "token_pos" .= tokenN pos
                                  , "char_pos" .= charOffset pos
                                  ]  | pos <- poss ]
                          ]
                        | (term, poss) <- M.toList scoredTermPositions
                        ]
                  ]
                | ScoredDocument {..} <- scores
                , let DocInfo {..} = scoredDocumentInfo
                ]
              ]
            | (qid, scores) <- M.toList results
            ]
        return ()

newtype DocumentFrequency = DocumentFrequency Int
                          deriving (Show, Eq, Ord, Binary)
instance Monoid DocumentFrequency where
    mempty = DocumentFrequency 0
    DocumentFrequency a `mappend` DocumentFrequency b = DocumentFrequency (a+b)

type ArchiveName = T.Text

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

normalizationPipeline
    :: Monad m
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
