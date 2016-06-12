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

import Data.Binary
import qualified Data.Aeson as Aeson
import Data.Aeson ((.=))
import Numeric.Log hiding (sum)
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.HashSet as HS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Control.Foldl as Foldl
import           Control.Foldl (Fold)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as V.G
import qualified Data.Vector.Unboxed as V.U
import           Data.Vector.Algorithms.Heap (sort)

import           Pipes
import           Pipes.Safe
import qualified Pipes.Text.Encoding as P.T
import qualified Pipes.ByteString as P.BS
import qualified Pipes.Prelude as P.P

import Options.Applicative

import qualified Data.SmallUtf8 as Utf8
import Utils
import AccumPostings
import Types
import Term
import Tokenise
import DataSource
import qualified BTree.File as BTree
import qualified DiskIndex.Posting as PostingIdx
import qualified DiskIndex.Document as DocIdx
import TopK
import qualified SimplIR.TREC as Trec
import qualified SimplIR.TrecStreaming as Kba
import RetrievalModels.QueryLikelihood

type QueryId = String
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
    parse "kba"    = kbaDocuments
    parse "robust" = trecDocuments
    parse _        = fail "unknown document source type"

streamMode :: Parser (IO ())
streamMode =
    score
      <$> optQueryFile
      <*> option auto (metavar "N" <> long "count" <> short 'n' <> value 10)
      <*> option str (metavar "FILE" <> long "stats" <> short 's'
                      <> help "background corpus statistics file")
      <*> option str (metavar "FILE" <> long "output" <> short 'o'
                      <> help "output file name")
      <*> optDocumentSource
      <*> inputFiles

indexMode :: Parser (IO ())
indexMode =
    buildIndex
      <$> optDocumentSource
      <*> inputFiles

corpusStatsMode :: Parser (IO ())
corpusStatsMode =
    corpusStats
      <$> optQueryFile
      <*> option str (metavar "FILE" <> long "output" <> short 'o'
                      <> help "output file path")
      <*> optDocumentSource
      <*> inputFiles


modes :: Parser (IO ())
modes = subparser
    $  command "score" (info streamMode fullDesc)
    <> command "corpus-stats" (info corpusStatsMode fullDesc)
    <> command "index" (info indexMode fullDesc)

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

main :: IO ()
main = do
    mode <- execParser $ info (helper <*> modes) fullDesc
    mode

corpusStats :: QueryFile -> StatsFile -> DocumentSource -> IO [DataSource] -> IO ()
corpusStats queryFile outputFile docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    let queryTerms = foldMap S.fromList queries
    runSafeT $ do
        stats <-
                foldProducer (Foldl.generalize foldCorpusStats)
             $  docSource docs >-> normalizationPipeline
            >-> cat'                                @((ArchiveName, DocumentName, DocumentLength), [(Term, Position)])
            >-> P.P.map (second $ map fst)
            >-> cat'                                @((ArchiveName, DocumentName, DocumentLength), [Term])
            >-> P.P.map (second $ filter ((`S.member` queryTerms)))

        liftIO $ putStrLn $ "Indexed "++show (corpusCollectionLength stats)
                          ++" documents with "++show (corpusCollectionSize stats)++" terms"
        liftIO $ BS.L.writeFile outputFile $ encode stats

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

foldCorpusStats :: Foldl.Fold ((ArchiveName, DocumentName, DocumentLength), [Term]) CorpusStats
foldCorpusStats =
    CorpusStats
      <$> lmap (length . snd) Foldl.sum
      <*> Foldl.length

foldTermStats :: Foldl.Fold [Term] (M.Map Term (TermFrequency, DocumentFrequency))
foldTermStats =
    M.mergeWithKey (\_ x y -> Just (x,y)) (fmap (\x -> (x, mempty))) (fmap (\y -> (mempty, y)))
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

type ScoredDocument = (Score, (ArchiveName, DocumentName, DocumentLength, M.Map Term [Position]))

score :: QueryFile -> Int -> FilePath -> FilePath -> DocumentSource -> IO [DataSource] -> IO ()
score queryFile resultCount statsFile outputRoot docSource readDocLocs = do
    docs <- readDocLocs
    queries <- readQueries queryFile
    let allQueryTerms = foldMap S.fromList queries

    -- load background statistics
    CorpusStats collLength collSize <- decode <$> BS.L.readFile statsFile
    termFreqs <- BTree.open (BTree.BTreePath "index/term-freqs" :: BTree.BTreePath Term (TermFrequency, DocumentFrequency))
    let getTermFreq term = maybe mempty snd $ BTree.lookup termFreqs term
        smoothing =
            Dirichlet 2500 $ \term ->
                case BTree.lookup termFreqs term of
                  Just (tf, _) ->
                      let n = getTermFrequency tf
                      in n / (realToFrac collLength)
                  Nothing ->
                      0.5 / (realToFrac collLength)

    let queriesFold :: Foldl.Fold ((ArchiveName, DocumentName, DocumentLength), M.Map Term [Position])
                                  (M.Map QueryId [ScoredDocument])
        queriesFold = traverse queryFold queries

        queryFold :: [Term] -> Foldl.Fold ((ArchiveName, DocumentName, DocumentLength), M.Map Term [Position])
                                          [ScoredDocument]
        queryFold queryTerms =
              Foldl.handles (Foldl.filtered (\(_, docTerms) -> not $ S.null $ M.keysSet queryTerms' `S.intersection` M.keysSet docTerms))
            $ lmap scoreTerms
            $ topK resultCount
          where
            queryTerms' :: M.Map Term Int
            queryTerms' = M.fromListWith (+) $ zip queryTerms (repeat 1)

            scoreTerms :: ((ArchiveName, DocumentName, DocumentLength), M.Map Term [Position])
                       -> ScoredDocument
            scoreTerms ((archive, docName, docLength), docTerms) =
                ( queryLikelihood smoothing (M.assocs queryTerms') docLength (M.toList $ fmap length docTerms)
                , (archive, docName, docLength, docTerms)
                )

    runSafeT $ do
        results <-
                foldProducer (Foldl.generalize queriesFold)
             $  docSource docs
            >-> normalizationPipeline
            >-> cat'                         @((ArchiveName, DocumentName, DocumentLength), [(Term, Position)])
            >-> P.P.map (second $ filter ((`S.member` allQueryTerms) . fst))
            >-> P.P.map (second $ M.fromListWith (++) . map (second (:[])))
            >-> cat'                         @((ArchiveName, DocumentName, DocumentLength), M.Map Term [Position])

        liftIO $ writeFile (outputRoot<.>"run") $ unlines
            [ unwords [ qid, T.unpack archive, Utf8.toString docName, show rank, show score, "simplir" ]
            | (qid, scores) <- M.toList results
            , (rank, (Exp score, (archive, DocName docName, _, _))) <- zip [1..] scores
            ]

        liftIO $ BS.L.writeFile (outputRoot<.>"json") $ Aeson.encode
            [ Aeson.object
              [ "query_id" .= qid
              , "results"  .=
                [ Aeson.object
                  [ "doc_name" .= docName
                  , "length"   .= docLength
                  , "archive"  .= archive
                  , "score"    .= score
                  , "postings" .= [
                        Aeson.object
                          [ "term" .= term
                          , "positions" .= [
                                Aeson.object
                                  [ "token_pos" .= tokenN pos
                                  , "char_pos" .= charOffset pos
                                  ]  | pos <- poss ]
                          ]
                        | (term, poss) <- M.toList postings
                        ]
                  ]
                | (Exp score, (archive, DocName docName, docLength, postings)) <- scores
                ]
              ]
            | (qid, scores) <- M.toList results
            ]
        return ()

buildIndex :: DocumentSource -> IO [DataSource] -> IO ()
buildIndex docSource readDocLocs = do
    docs <- readDocLocs

    let chunkIndexes :: Producer (FragmentIndex (V.U.Vector Position)) (SafeT IO) ()
        chunkIndexes =
                foldChunks 1000 (Foldl.generalize $ indexPostings (TermFreq . V.G.length))
             $  docSource docs
            >-> normalizationPipeline
            >-> cat'                                              @( (ArchiveName, DocumentName, DocumentLength)
                                                                   , [(Term, Position)] )
            >-> zipWithList [DocId 0..]
            >-> cat'                                              @( DocumentId
                                                                   , ( (ArchiveName, DocumentName, DocumentLength)
                                                                     , [(Term, Position)] )
                                                                   )
            >-> P.P.map (\(docId, ((archive, docName, docLen), postings)) ->
                           ((docId, archive, docName, docLen), (docId, postings)))
            >-> P.P.map (second $ \(docId, postings) ->
                            toPostings docId $ M.assocs $ foldTokens accumPositions postings
                        )
            >-> cat'                                              @( (DocumentId, ArchiveName, DocumentName, DocumentLength)
                                                                   , [(Term, Posting (V.U.Vector Position))]
                                                                   )

    chunks <- runSafeT $ P.P.toListM $ for (chunkIndexes >-> zipWithList [0..]) $ \(n, (docIds, postings, termFreqs, corpusStats)) -> do
        let indexPath = "index-"++show n
        liftIO $ print (n, M.size docIds)

        let postingsPath :: PostingIdx.PostingIndexPath (V.U.Vector Position)
            postingsPath = PostingIdx.PostingIndexPath $ indexPath </> "postings"
        liftIO $ PostingIdx.fromTermPostings 1024 postingsPath (fmap V.toList postings)

        let docsPath :: DocIdx.DocIndexPath (ArchiveName, DocumentName, DocumentLength)
            docsPath = DocIdx.DocIndexPath $ indexPath </> "documents"
        liftIO $ DocIdx.write docsPath docIds

        let termFreqsPath :: BTree.BTreePath Term (TermFrequency, DocumentFrequency)
            termFreqsPath = BTree.BTreePath $ indexPath </> "term-freqs"
        liftIO $ BTree.fromOrdered (fromIntegral $ M.size termFreqs) termFreqsPath (each $ M.toList termFreqs)

        yield (postingsPath, docsPath, termFreqsPath, corpusStats)

    mergeIndexes chunks

mergeIndexes :: [( PostingIdx.PostingIndexPath (V.U.Vector Position)
                 , DocIdx.DocIndexPath (ArchiveName, DocumentName, DocumentLength)
                 , BTree.BTreePath Term (TermFrequency, DocumentFrequency)
                 , CorpusStats
                 )]
             -> IO ()
mergeIndexes chunks = do
    docIds0 <- DocIdx.merge (DocIdx.DocIndexPath "out/documents") $ map (\(_,docs,_,_) -> docs) chunks
    PostingIdx.merge (PostingIdx.PostingIndexPath "out/postings") $ zip docIds0 $ map (\(postings,_,_,_) -> postings) chunks
    BTree.merge mappend (BTree.BTreePath "out/term-freqs") $ map (\(_,_,termFreqs,_) -> termFreqs) chunks

    BS.L.writeFile ("index" </> "coll-length") $ encode $ foldMap (\(_,_,_,x) -> x) chunks
    return ()

offlineMerge :: [FilePath] -> IO ()
offlineMerge = mergeIndexes . map toChunk
  where
    toChunk indexPath = (postingsPath, docsPath, termFreqsPath, undefined)
      where
        postingsPath = PostingIdx.PostingIndexPath $ indexPath </> "postings"
        docsPath = DocIdx.DocIndexPath $ indexPath </> "documents"
        termFreqsPath = BTree.BTreePath $ indexPath </> "term-freqs"

newtype DocumentFrequency = DocumentFrequency Int
                          deriving (Show, Eq, Ord, Binary)
instance Monoid DocumentFrequency where
    mempty = DocumentFrequency 0
    DocumentFrequency a `mappend` DocumentFrequency b = DocumentFrequency (a+b)

type SavedPostings p = M.Map Term (V.Vector (Posting p))
type FragmentIndex p =
    ( M.Map DocumentId (ArchiveName, DocumentName, DocumentLength)
    , SavedPostings p
    , M.Map Term (TermFrequency, DocumentFrequency)
    , CorpusStats
    )

indexPostings :: forall p. (Ord p)
              => (p -> TermFrequency)
              -> Fold ( (DocumentId, ArchiveName, DocumentName, DocumentLength)
                      , [(Term, Posting p)])
                      (FragmentIndex p)
indexPostings getTermFreq =
    (,,,)
      <$> lmap fst docMeta
      <*> lmap snd postings
      <*> lmap snd termFreqs
      <*> lmap fst corpusStats
  where
    docMeta  = lmap (\(docId, archive, docName, docLen) ->
                       M.singleton docId (archive, docName, docLen)
                    ) Foldl.mconcat
    postings = fmap (M.map $ V.G.modify sort . fromFoldable) foldPostings

    termFreqs :: Fold [(Term, Posting p)] (M.Map Term (TermFrequency, DocumentFrequency))
    termFreqs = Foldl.handles traverse
                $ lmap (\(term, ps) -> M.singleton term (getTermFreq $ postingBody ps, DocumentFrequency 1))
                $ mconcatMaps

    corpusStats =
        CorpusStats
          <$> collLength
          <*> Foldl.length
    collLength = lmap (\(_, _, _, DocLength c) -> c) Foldl.sum

type ArchiveName = T.Text

trecDocuments :: [DataSource]
              -> Producer ((ArchiveName, DocumentName), T.Text) (SafeT IO) ()
trecDocuments dsrcs =
    mapM_ (\dsrc -> Trec.trecDocuments' (P.T.decodeUtf8 $ dataSource dsrc)
                    >-> P.P.map (\d -> ( ( getFileName $ dsrcLocation dsrc
                                         , DocName $ Utf8.fromText $ Trec.docNo d)
                                       , Trec.docText d)))
          dsrcs

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
            ((ArchiveName, DocumentName, DocumentLength), [(Term, Position)]) m ()
normalizationPipeline =
          cat'                                          @((ArchiveName, DocumentName), T.Text)
      >-> P.P.map (fmap $ T.map killPunctuation)
      >-> P.P.map (fmap tokeniseWithPositions)
      >-> cat'                                          @((ArchiveName, DocumentName), [(T.Text, Position)])
      >-> P.P.map (\((archive, docName), terms) ->
                      let docLen = DocLength $ length $ filter (not . T.all (not . isAlphaNum) . fst) terms
                      in ((archive, docName, docLen), terms))
      >-> cat'                                          @( (ArchiveName, DocumentName, DocumentLength)
                                                         , [(T.Text, Position)])
      >-> P.P.map (fmap normTerms)
      >-> cat'                                          @( (ArchiveName, DocumentName, DocumentLength)
                                                         , [(Term, Position)])
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

fromFoldable :: (Foldable f, V.G.Vector v a)
             => f a -> v a
fromFoldable xs = V.G.fromListN (length xs) (toList xs)
