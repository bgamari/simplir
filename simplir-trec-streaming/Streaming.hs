{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DeriveTraversable #-}
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
import qualified Data.Yaml as Yaml
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.HashSet as HS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Control.Foldl as Foldl

import           Pipes
import           Pipes.Safe
import qualified Pipes.Prelude as P.P

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import SimplIR.Utils
import Control.Foldl.Map
import SimplIR.Types
import SimplIR.Term as Term
import SimplIR.Tokenise
import SimplIR.DataSource as DataSource
import SimplIR.BinaryFile as BinaryFile
import qualified BTree.File as BTree
import SimplIR.TopK
import qualified SimplIR.TrecStreaming as Kba
import SimplIR.RetrievalModels.QueryLikelihood as QL

import ReadKba
import qualified Fac.Types as Fac
import Types
import Query
import Parametric
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

type DocumentSource = [DataSource] -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()

streamMode :: Parser (IO ())
streamMode =
    scoreStreaming
      <$> optQueryFile
      <*> option (Fac.diskIndexPaths <$> str) (metavar "DIR" <> long "fac-index" <> short 'f')
      <*> option auto (metavar "N" <> long "count" <> short 'n')
      <*> option (corpusStatsPaths <$> str)
                 (metavar "PATH" <> long "stats" <> short 's'
                 <> help "background corpus statistics index")
      <*> option str (metavar "PATH" <> long "output" <> short 'o'
                      <> help "output file name")
      <*> pure kbaDocuments
      <*> inputFiles

mergeCorpusStatsMode :: Parser (IO ())
mergeCorpusStatsMode =
    mergeCorpusStats
      <$> option (corpusStatsPaths <$> str) (metavar "DIR" <> long "output" <> short 'o'
                                             <> help "output file path")
      <*> some (argument (corpusStatsPaths <$> str)
                         (metavar "DIR" <> help "corpus statistics indexes to merge"))

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
    <> command "merge-corpus-stats" (info mergeCorpusStatsMode fullDesc)

type QueryFile = FilePath

optQueryFile :: Parser QueryFile
optQueryFile =
    option str (metavar "FILE" <> long "query" <> short 'q' <> help "query file")

newtype WikiId = WikiId Utf8.SmallUtf8
               deriving (Show, Eq, Ord)


readQueries :: QueryFile -> IO (M.Map QueryId QueryNode)
readQueries fname = do
    Just queries' <- Yaml.decodeFile fname
    let queries = getQueries queries'
    let allTerms = foldMap (S.fromList . collectFieldTerms FieldText) queries
    hPutStrLn stderr $ show (M.size queries)++" queries with "++show (S.size allTerms)++" unique terms"
    return queries

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

corpusStats :: QueryFile -> CorpusStatsPaths -> DocumentSource -> IO [DataSource] -> IO ()
corpusStats queryFile output docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    createDirectoryIfMissing True (corpusStatsRoot output)
    let allQueryTerms = foldMap (S.fromList . collectFieldTerms FieldText) queries
    runSafeT $ do
        (corpusStats, termStats) <-
                foldProducer (Foldl.generalize foldCorpusStats)
             $  docSource docs >-> normalizationPipeline
            >-> cat'                                @(DocumentInfo, [(Term, Position)])
            >-> P.P.map (second $ filter (`S.member` allQueryTerms) . map fst)
            >-> cat'                                @(DocumentInfo, [Term])

        liftIO $ putStrLn $ "Indexed "++show (corpusCollectionLength corpusStats)
                          ++" tokens with "++show (corpusCollectionSize corpusStats)++" documents"
        liftIO $ BinaryFile.write (diskCorpusStats output) corpusStats
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size termStats) (diskTermStats output) (each $ M.assocs termStats)

mergeCorpusStats :: CorpusStatsPaths -> [CorpusStatsPaths] -> IO ()
mergeCorpusStats output statss = do
    createDirectoryIfMissing True (corpusStatsRoot output)
    BinaryFile.write (diskCorpusStats output) =<< BinaryFile.mconcat (map diskCorpusStats statss)
    BTree.merge mappend (diskTermStats output) (map diskTermStats statss)

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


interpretQuery :: Distribution Term
               -> Distribution Fac.EntityId
               -> Parameters Double
               -> QueryNode
               -> (DocumentInfo, M.Map Term [Position], (DocumentLength, M.Map Fac.EntityId TermFrequency))
               -> (Score, M.Map RecordedValueName Double)
interpretQuery termBg entityBg params node0 doc = go node0
  where
    go :: QueryNode -> (Score, M.Map RecordedValueName Double)
    go SumNode {..}       = first getSum $ foldMap (first Sum . go) children
    go ProductNode {..}   = first getProduct $ foldMap (first Product . go) children
    go ScaleNode {..}     = first (s *) $ go child
      where s = realToFrac $ runParametricOrFail params scalar
    go RetrievalNode {..} =
        let score = case retrievalModel of
              QueryLikelihood smoothing ->
                  case field of
                    FieldText ->
                        let queryTerms = M.fromListWith (+) $ zip terms (repeat 1)
                            docTerms = M.toList $ fmap length docTermPositions
                            smooth = runParametricOrFail params smoothing $ termBg
                        in QL.queryLikelihood smooth (M.assocs queryTerms) (docLength info) docTerms

                    FieldFreebaseIds ->
                        let queryTerms = M.fromListWith (+) $ zip terms (repeat 1)
                            docTerms = M.toList $ fmap fromEnum entityFreqs
                            smooth = runParametricOrFail params smoothing $ entityBg
                        in QL.queryLikelihood smooth (M.assocs queryTerms) (docLength info) docTerms
            (info, docTermPositions, (_entityDocLen, entityFreqs)) = doc
        in (score, mempty)

queryFold :: Distribution Term
          -> Distribution Fac.EntityId
          -> Parameters Double
          -> Int                        -- ^ how many results should we collect?
          -> QueryNode
          -> Foldl.Fold (DocumentInfo, M.Map Term [Position], (DocumentLength, M.Map Fac.EntityId TermFrequency))
                        [ScoredDocument]
queryFold termBg entityBg params resultCount query =
      --Foldl.handles (Foldl.filtered (\(_, docTerms, _) -> not $ S.null $ M.keysSet queryTerms' `S.intersection` M.keysSet docTerms)) -- TODO: Should we do this?
    lmap scoreQuery
    $ topK resultCount
  where
    scoreQuery :: (DocumentInfo, M.Map Term [Position], (DocumentLength, M.Map Fac.EntityId TermFrequency))
               -> ScoredDocument
    scoreQuery doc@(info, docTermPositions, (entityDocLen, entityFreqs)) =
        ScoredDocument { scoredRankScore = score
                       , scoredDocumentInfo = info
                       , scoredTermPositions = docTermPositions
                       , scoredEntityFreqs = entityFreqs
                       , scoredRecordedValues = recorded
                       }
      where
        (score, recorded) = interpretQuery termBg entityBg params query doc

scoreStreaming :: QueryFile -> Fac.DiskIndex -> Int -> CorpusStatsPaths -> FilePath -> DocumentSource -> IO [DataSource] -> IO ()
scoreStreaming queryFile facIndexPath resultCount background outputRoot docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    let allQueryTerms = foldMap (S.fromList . collectFieldTerms FieldText) queries

    -- load FAC annotations
    facIndex <- BTree.open $ Fac.diskDocuments facIndexPath
    facEntityIdStats <- BTree.open $ Fac.diskTermStats facIndexPath
    facCorpusStats <- BinaryFile.read $ Fac.diskCorpusStats facIndexPath

    -- load background statistics
    CorpusStats collLength _collSize <- BinaryFile.read (diskCorpusStats background)
    termFreqs <- BTree.open (diskTermStats background)
    let termBg :: Distribution Term
        termBg term =
            case BTree.lookup termFreqs term of
              Just (TermStats tf _) -> getTermFrequency tf / realToFrac collLength
              Nothing               -> 0.5 / realToFrac collLength

        entityBg :: Distribution Fac.EntityId
        entityBg entity =
            let collLength = Fac.corpusCollectionLength facCorpusStats
            in case BTree.lookup facEntityIdStats entity of
                  Just (Fac.TermStats tf _) -> getTermFrequency tf / realToFrac collLength
                  Nothing                   -> 0.05 / realToFrac collLength

    Just paramSets <- fmap getParamSets <$> Yaml.decodeFile "parameters.json"
                   :: IO (Maybe (M.Map ParamSettingName (Parameters Double)))

    let queriesFold :: Foldl.Fold (DocumentInfo, M.Map Term [Position], (DocumentLength, M.Map Fac.EntityId TermFrequency))
                                  (M.Map (QueryId, ParamSettingName) [ScoredDocument])
        queriesFold = sequenceA $ M.fromList
                      [ ((queryId, paramSetting), queryFold termBg entityBg params resultCount queryNode)
                      | (paramSetting, params) <- M.assocs paramSets
                      , (queryId, queryNode) <- M.assocs queries
                      ]

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

        -- TODO
        --liftIO $ BS.L.writeFile (outputRoot<.>"json") $ Aeson.encode results
        return ()

newtype DocumentFrequency = DocumentFrequency Int
                          deriving (Show, Eq, Ord, Binary)
instance Monoid DocumentFrequency where
    mempty = DocumentFrequency 0
    DocumentFrequency a `mappend` DocumentFrequency b = DocumentFrequency (a+b)

kbaDocuments :: [DataSource]
             -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
kbaDocuments dsrcs =
    mapM_ (\src -> do
                liftIO $ hPutStrLn stderr $ show src
                bs <- lift $ readKbaFile src
                mapM_ (yield . (getFilePath $ dsrcLocation src,))
                      (Kba.readItems $ BS.L.toStrict bs)
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
