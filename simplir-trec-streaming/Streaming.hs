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
import Data.Foldable (toList)
import Data.Either (partitionEithers)
import Data.Maybe
import Data.Semigroup hiding (option)
import Data.Profunctor
import Data.Char
import GHC.Generics
import System.IO
import System.FilePath
import System.Directory (createDirectoryIfMissing)

import Data.Binary
import qualified Data.Yaml as Yaml
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Vector.Unboxed as VU
import qualified Data.Map.Strict as M
import qualified Data.HashMap.Strict as HM
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Lazy as T.L
import qualified Codec.Compression.GZip as GZip
import qualified Control.Foldl as Foldl

import           Pipes
import           Pipes.Safe
import qualified Pipes.Prelude as P.P
import qualified Pipes.Text as P.T
import qualified Pipes.Text.Encoding as P.T.E

import Options.Applicative hiding ((<>))

import qualified Data.SmallUtf8 as Utf8
import SimplIR.Utils
import Control.Foldl.Map
import SimplIR.Types
import SimplIR.Term as Term
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
import qualified Data.Trie as Trie
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
      <*> optParamsFile
      <*> optional (option (Fac.diskIndexPaths <$> str)
                           (metavar "DIR" <> long "fac-index" <> short 'f'))
      <*> option auto (metavar "N" <> long "count" <> short 'n')
      <*> option (corpusStatsPaths <$> str)
                 (metavar "PATH" <> long "stats" <> short 's'
                 <> help "background corpus statistics index")
      <*> option str (metavar "PATH" <> long "output" <> short 'o'
                      <> help "output file name")
      <*> pure kbaDocuments  -- testDocuments
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
      <*> pure kbaDocuments  -- testDocuments
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

type ParamsFile = FilePath

optParamsFile :: Parser ParamsFile
optParamsFile =
    option str (metavar "FILE" <> long "params" <> short 'p' <> help "parameters file")

newtype WikiId = WikiId Utf8.SmallUtf8
               deriving (Show, Eq, Ord)


readQueries :: QueryFile -> IO (M.Map QueryId QueryNode)
readQueries fname = do
    queries' <- either decodeError pure =<< Yaml.decodeFileEither fname
    let queries = getQueries queries'
    let allTerms = foldMap (S.fromList . collectFieldTerms FieldText) queries
    hPutStrLn stderr $ show (M.size queries)++" queries with "++show (S.size allTerms)++" unique terms"
    return queries
  where
    decodeError exc = fail $ "Failed to parse queries file "++fname++": "++show exc

readParameters :: QueryFile -> IO (M.Map ParamSettingName (Parameters Double))
readParameters fname = do
    either paramDecodeError (pure . getParamSets) =<< Yaml.decodeFileEither fname
  where
    paramDecodeError exc = fail $ "Failed to read parameters file "

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

corpusStats :: QueryFile -> CorpusStatsPaths -> DocumentSource -> IO [DataSource] -> IO ()
corpusStats queryFile output docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    createDirectoryIfMissing True (corpusStatsRoot output)
    let (allQueryTerms, allPhrases) =
            first S.fromList
            $ partitionEithers $ map toEither
            $ foldMap (collectFieldTerms FieldText) queries
          where toEither (Token  x) = Left  x
                toEither (Phrase x) = Right x

    runSafeT $ do
        (corpusStats, termStats) <-
                foldProducer (Foldl.generalize foldCorpusStats)
             $  docSource docs >-> normalizationPipeline
            >-> cat'                                @(DocumentInfo, [(Term, Position)])
            >-> P.P.map (second $ \terms ->
                            map (Phrase . fst) (findPhrases allPhrases terms)
                        ++ (map Token . filter (`S.member` allQueryTerms) . map fst) terms)
            >-> cat'                                @(DocumentInfo, [TokenOrPhrase Term])

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
                                         , diskTermStats   :: BTree.BTreePath (TokenOrPhrase Term) TermStats
                                         }

corpusStatsPaths :: FilePath -> CorpusStatsPaths
corpusStatsPaths root =
    CorpusStatsPaths { corpusStatsRoot = root
                     , diskCorpusStats = BinaryFile $ root </> "corpus-stats"
                     , diskTermStats = BTree.BTreePath $ root </> "term-freqs"
                     }

foldCorpusStats :: Foldl.Fold (DocumentInfo, [TokenOrPhrase Term]) (CorpusStats, M.Map (TokenOrPhrase Term) TermStats)
foldCorpusStats =
    (,)
      <$> lmap fst foldCorpusStats'
      <*> lmap snd foldTermStats

foldCorpusStats' :: Foldl.Fold DocumentInfo CorpusStats
foldCorpusStats' =
    CorpusStats
      <$> lmap (fromEnum . docLength) Foldl.sum
      <*> Foldl.length

foldTermStats :: Foldl.Fold [TokenOrPhrase Term] (M.Map (TokenOrPhrase Term) TermStats)
foldTermStats =
    M.mergeWithKey (\_ x y -> Just (TermStats x y)) (fmap (\x -> TermStats x mempty)) (fmap (\y -> TermStats mempty y))
      <$> termFreqs
      <*> docFreqs
  where
    docFreqs :: Foldl.Fold [TokenOrPhrase Term] (M.Map (TokenOrPhrase Term) DocumentFrequency)
    docFreqs = lmap (\terms -> M.fromList $ zip terms (repeat $ DocumentFrequency 1)) mconcatMaps
    termFreqs :: Foldl.Fold [TokenOrPhrase Term] (M.Map (TokenOrPhrase Term) TermFrequency)
    termFreqs =
          Foldl.handles traverse
        $ lmap (\term -> M.singleton term (TermFreq 1))
        $ mconcatMaps

-- | A document to be scored
type DocForScoring = (DocumentInfo, M.Map (TokenOrPhrase Term) (VU.Vector Position), (DocumentLength, M.Map Fac.EntityId TermFrequency))

interpretQuery :: Distribution (TokenOrPhrase Term)
               -> Distribution Fac.EntityId
               -> Parameters Double
               -> QueryNode
               -> DocForScoring
               -> Maybe (Score, M.Map RecordedValueName Yaml.Value)
interpretQuery termBg entityBg params node0 = go node0
  where
    recording :: Maybe RecordedValueName -> Maybe (Score, M.Map RecordedValueName Yaml.Value)
              -> Maybe (Score, M.Map RecordedValueName Yaml.Value)
    recording mbName =
        fmap (\(score, r) -> (score, maybe id (\name -> M.insert name (Yaml.toJSON score)) mbName $ r))

    foldAllMaybes :: Monoid a => (QueryNode -> Maybe a) -> [QueryNode] -> Maybe a
    foldAllMaybes f = go' mempty
      where
        go' acc  (x : xs) =
            case f x of
              Just y  -> go' (y `mappend` acc) xs
              Nothing -> Nothing
        go' acc  []       = Just acc

    go :: QueryNode -> DocForScoring
       -> Maybe (Score, M.Map RecordedValueName Yaml.Value)
    go ConstNode {..}     = let c = realToFrac $ runParametricOrFail params value
                            in \_ -> Just (c, mempty)
    go DropNode {}        = const Nothing
    go SumNode {..}       = \doc ->
        recording recordOutput
        $ fmap (first getSum)
        $ foldAllMaybes (fmap (first Sum) . flip go doc) children
    go ProductNode {..}   = \doc ->
        recording recordOutput
        $ fmap (first getProduct)
        $ foldAllMaybes (fmap (first Product) . flip go doc) children
    go ScaleNode {..}     = \doc ->
        recording recordOutput
        $ fmap (first (s *))
        $ go child doc
      where s = realToFrac $ runParametricOrFail params scalar
    go FeatureNode {..}   = \doc ->
        fmap (first (w *))
        $ recording (Just $ recordedFeatureName featureName)
        $ go child doc
      where w = realToFrac $ runParametricOrFail params (Parameter $ featureParameterName featureName)
    go RetrievalNode {..} =
        case retrievalModel of
          QueryLikelihood smoothing ->
              case field of
                FieldText ->
                    let queryTerms = M.fromListWith (+) $ toList terms
                        smooth = runParametricOrFail params smoothing $ termBg
                    in \doc ->
                        let (info, docTermPositions, _) = doc
                            docTerms = M.toList $ fmap VU.length docTermPositions
                            score = QL.queryLikelihood smooth (M.assocs queryTerms) (docLength info) (map (second realToFrac) docTerms)
                            recorded = mempty -- TODO
                        in Just (score, recorded)

                FieldFreebaseIds ->
                    let queryTerms = M.fromListWith (+) $ toList terms
                        smooth = runParametricOrFail params smoothing $ entityBg
                    in \doc ->
                        let (info, _, (_entityDocLen, entityFreqs)) = doc
                            docTerms = M.toList $ fmap fromEnum entityFreqs
                            score = QL.queryLikelihood smooth (M.assocs queryTerms) (docLength info) (map (second realToFrac) docTerms)
                            recorded = mempty -- TODO
                        in Just (score, recorded)
    go CondNode {..} = \doc@(docinfo, text, entities) ->
        let found = all (`M.member` text) predicateTerms
        in if found `xor` predicateNegated
             then go trueChild doc
             else go falseChild doc

xor :: Bool -> Bool -> Bool
xor True  = not
xor False = id

queryFold :: Distribution (TokenOrPhrase Term)
          -> Distribution Fac.EntityId
          -> Parameters Double
          -> Int                        -- ^ how many results should we collect?
          -> QueryNode
          -> Foldl.Fold (DocumentInfo, M.Map (TokenOrPhrase Term) (VU.Vector Position), (DocumentLength, M.Map Fac.EntityId TermFrequency))
                        [ScoredDocument]
queryFold termBg entityBg params resultCount query =
    Foldl.handles (Foldl.filtered docFilter)
    $ lmap scoreQuery
    $ Foldl.handles Foldl.folded
    $ topK resultCount
  where
    queryTerms = S.fromList $ collectFieldTerms FieldText query
    queryEntities = S.fromList $ collectFieldTerms FieldFreebaseIds query

    -- TODO: Should we do this?
    -- Take only documents that match in at least one field.
    docFilter (_, docTerms, (_, docEntities)) = textMatches || entitiesMatches
      where
        textMatches = not $ S.null $ queryTerms `S.intersection` M.keysSet docTerms
        entitiesMatches = not $ S.null $ queryEntities `S.intersection` M.keysSet docEntities

    scoreQuery :: (DocumentInfo, M.Map (TokenOrPhrase Term) (VU.Vector Position), (DocumentLength, M.Map Fac.EntityId TermFrequency))
               -> Maybe ScoredDocument
    scoreQuery doc@(info, docTermPositions, (entityDocLen, entityFreqs)) =
        fmap toScoredDocument
        $ interpretQuery termBg entityBg params query doc
      where
        toScoredDocument (score, recorded) =
            ScoredDocument { scoredRankScore = score
                          , scoredDocumentInfo = info
                          , scoredTermPositions = docTermPositions `M.intersection` M.fromSet (const ()) queryTerms
                          , scoredEntityFreqs = entityFreqs `M.intersection` M.fromSet (const ()) queryEntities
                          , scoredRecordedValues = recorded
                          }

scoreStreaming :: QueryFile             -- ^ queries
               -> ParamsFile            -- ^ query parameter setting
               -> Maybe Fac.DiskIndex   -- ^ FAC index
               -> Int                   -- ^ desired result count
               -> CorpusStatsPaths      -- ^ corpus background statistics
               -> FilePath              -- ^ output path
               -> DocumentSource        -- ^
               -> IO [DataSource]       -- ^ an action to read the list of documents to score
               -> IO ()
scoreStreaming queryFile paramsFile facIndexPath resultCount background outputRoot docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    let allQueryTerms = foldMap (S.fromList . collectFieldTerms FieldText) queries
        allQueryPhrases = [ phrase
                          | query <- toList queries
                          , Phrase phrase <- collectFieldTerms FieldText query
                          ]
        allQueryEntities = foldMap (S.fromList . collectFieldTerms FieldFreebaseIds) queries

    -- load FAC annotations
    (entityBg, lookupFac) <-
        case facIndexPath of
          Nothing -> return (const 0, const (toEnum 0, mempty))
          Just path -> do
            facIndex <- BTree.open $ Fac.diskDocuments path
            facEntityIdStats <- BTree.open $ Fac.diskTermStats path
            facCorpusStats <- BinaryFile.read $ Fac.diskCorpusStats path
            let lookupFac :: DocumentName
                          -> (DocumentLength, M.Map Fac.EntityId TermFrequency)
                lookupFac docName =
                    maybe (DocLength 0, M.empty) (first Fac.docLength)
                    $ BTree.lookup facIndex docName

            let entityBgMap = HM.fromList $ map (\e -> (e, lookupEntity e)) $ S.toList allQueryEntities
                    where
                      lookupEntity :: Distribution Fac.EntityId
                      lookupEntity entity = {-# SCC entityBg #-}
                          let collLength = Fac.corpusCollectionLength facCorpusStats
                          in case BTree.lookup facEntityIdStats entity of
                                Just (Fac.TermStats tf _) -> getTermFrequency tf / realToFrac collLength
                                Nothing                   -> 0.05 / realToFrac collLength
                entityBg = (entityBgMap HM.!)
            return (entityBg, lookupFac)

    -- load text background statistics
    CorpusStats collLength _collSize <- BinaryFile.read (diskCorpusStats background)
    termFreqs <- BTree.open (diskTermStats background)
    let termBgMap = HM.fromList $ map (\t -> (t, lookupTerm t)) $ S.toList allQueryTerms
          where
            lookupTerm :: Distribution (TokenOrPhrase Term)
            lookupTerm term = {-# SCC termBg #-}
                case BTree.lookup termFreqs term of
                  Just (TermStats tf _) -> getTermFrequency tf / realToFrac collLength
                  Nothing               -> 0.5 / realToFrac collLength
        termBg = (termBgMap HM.!)

    paramSets <- readParameters paramsFile

    let queriesFold :: Foldl.Fold (DocumentInfo, M.Map (TokenOrPhrase Term) (VU.Vector Position), (DocumentLength, M.Map Fac.EntityId TermFrequency))
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
            >-> P.P.map (second $ \terms ->
                            [ (Phrase phrase, pos) | (phrase, pos) <- findPhrases allQueryPhrases terms ]
                            ++
                            [ (Token term, pos)
                            | (term, pos) <- terms
                            , Token term `S.member` allQueryTerms
                            ])
            >-> P.P.map (second $ fmap VU.fromList
                                . M.fromListWith (++)
                                . map (second (:[])))
            >-> cat'                         @(DocumentInfo, M.Map (TokenOrPhrase Term) (VU.Vector Position))
            >-> P.P.map (\(docInfo, termPostings) ->
                            let (facDocLen, entityIdPostings) = lookupFac (docName docInfo)
                            in (docInfo, termPostings, (facDocLen, entityIdPostings))
                        )
            >-> cat'                         @( DocumentInfo
                                              , M.Map (TokenOrPhrase Term) (VU.Vector Position)
                                              , (DocumentLength, M.Map Fac.EntityId TermFrequency)
                                              )

        liftIO $ putStrLn $ "Writing scored results to "++outputRoot++"..."
        liftIO $ BS.L.writeFile (outputRoot<.>"json.gz") $ GZip.compress
               $ Aeson.encode $ Results results
        liftIO $ putStrLn "done"
        return ()


-- | Find occurrences of a set of phrases in an ordered sequence of 'Term's.
findPhrases :: [[Term]] -> [(Term, Position)] -> [([Term], Position)]
findPhrases phrases terms =
    map mergeMatches $ Trie.matches' fst terms trie
  where
    trie = Trie.fromList $ map (\x -> (x,x)) phrases
    mergeMatches :: ([(Term, Position)], [Term]) -> ([Term], Position)
    mergeMatches (matchedTerms, phrase) =
        (phrase, fromMaybe (error "findPhrases: Empty phrase") $ getOption $ foldMap (Option . Just . snd) matchedTerms)

newtype DocumentFrequency = DocumentFrequency Int
                          deriving (Show, Eq, Ord, Binary)
instance Monoid DocumentFrequency where
    mempty = DocumentFrequency 0
    DocumentFrequency a `mappend` DocumentFrequency b = DocumentFrequency (a+b)

testDocuments :: [DataSource]
              -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
testDocuments dsrcs =
    mapM_ (\src -> do
                xs <- lift $ P.T.toLazyM $ void $ P.T.E.decodeUtf8 $ dataSource src
                let archive = "hi"
                    docs = map (\x -> let (name, content) = T.L.span (/= ':') x
                                          docName = DocName $ Utf8.fromText $ T.L.toStrict name
                                      in ((archive, docName), T.L.toStrict content)
                               ) (T.L.lines xs)
                mapM_ yield docs
          ) dsrcs



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
      >-> P.P.map (fmap kbaTokenise)
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
