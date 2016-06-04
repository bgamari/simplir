{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Monad.State.Strict hiding ((>=>))
import Data.Bifunctor
import Data.Monoid
import Data.Tuple
import Data.Char
import Numeric.Log

import Data.Binary
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Control.Foldl as Foldl
import System.FilePath

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
import qualified BTree
import RetrievalModels.QueryLikelihood

type QueryId = String

queryOpts :: Parser (Int, M.Map Term Int, QueryId, [DataLocation])
queryOpts =
    (,,,)
      <$> option auto (metavar "N" <> long "count" <> short 'n')
      <*> queryTerms
      <*> option str (metavar "QUERY_ID" <> long "qid" <> short 'i' <> value "1")
      <*> some (argument (LocalFile <$> str) (metavar "FILE" <> help "TREC input file"))
  where
    queryTerms :: Parser (M.Map Term Int)
    queryTerms = M.fromListWith (+) . flip zip (repeat 1)
                 <$> some (option (Term.fromString <$> str) (metavar "TERMS" <> long "query" <> short 'q'))

compression = Just Lzma

main :: IO ()
main = do
    (resultCount, queryTerms, qid, dsrcs) <- execParser $ info (helper <*> queryOpts) mempty
    score qid queryTerms resultCount dsrcs

score :: QueryId -> M.Map Term Int -> Int -> [DataLocation] -> IO ()
score qid queryTerms resultCount docs = do
    -- background statistics
    let indexPath = "index"
    Right tfIdx <- BTree.open (indexPath </> "term-freqs")
        :: IO (Either String (BTree.LookupTree Term TermFrequency))
    collLength <- decode <$> BS.L.readFile (indexPath </> "coll-length") :: IO Int
    let smoothing = Dirichlet 2500 ((\n -> (n + 0.5) / (realToFrac collLength + 1)) . maybe 0 getTermFrequency . BTree.lookup tfIdx)

    runSafeT $ do
        results <-
                foldProducer (Foldl.generalize $ topK resultCount)
             $  streamingDocuments docs
            >-> normalizationPipeline
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> P.P.filter (any (`M.member` queryTerms) . map fst . snd)
            >-> P.P.map (second $ \terms ->
                            let docLength = DocLength $ length terms
                                terms' = map (\(term,_) -> (term, 1)) terms
                            in queryLikelihood smoothing (M.assocs queryTerms) docLength terms'
                        )
            >-> P.P.map swap
            >-> cat'                                          @(Score, DocumentName)

        let toRunFile rank (Exp score, DocName docName) = unwords
                [ qid, "Q0", Utf8.toString docName, show rank, show score, "simplir" ]
        liftIO $ putStrLn $ unlines $ zipWith toRunFile [1..] results

streamingDocuments :: [DataLocation] -> Producer Trec.StreamItem (SafeT IO) ()
streamingDocuments dsrcs =
    mapM_ (\src -> do
                bs <- P.BS.toLazyM (decompress compression $ produce src)
                mapM_ yield (Trec.readItems $ BS.L.toStrict bs)
          ) dsrcs

normalizationPipeline :: Monad m => Pipe Trec.StreamItem (DocumentName, [(Term, Position)]) m ()
normalizationPipeline =
          cat'                                          @Trec.StreamItem
      >-> P.P.mapFoldable
              (\d -> do body <- Trec.body d
                        visible <- Trec.cleanVisible body
                        return ( DocName $ Utf8.fromText $ Trec.getDocumentId $ Trec.documentId d
                                , visible
                                ))
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
