{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Data.List (sort)
import Control.Monad.State.Strict hiding ((>=>))
import Data.Bifunctor
import Data.Foldable
import Data.Profunctor
import Data.Monoid
import Data.Tuple
import Data.Char

import Data.Binary
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.ByteString.Short as BS.S
import qualified Data.Map.Strict as M
import qualified Data.HashSet as HS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T.E
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy as T.L
import qualified Data.Vector.Unboxed as VU
import qualified Control.Foldl as Foldl
import System.FilePath

import           Pipes
import           Pipes.Safe
import qualified Pipes.Prelude as P.P
import qualified Pipes.Text.Encoding as P.T

import Options.Applicative

import Utils
import Types
import Term
import Progress
import Tokenise
import WarcDocSource
import AccumPostings
import DataSource
import TopK
import SimplIR.TREC as TREC
import DiskIndex
import qualified BTree
import RetrievalModels.QueryLikelihood

opts :: Parser (Int, T.Text, [DataLocation])
opts =
    (,,)
      <$> option auto (metavar "N" <> long "count" <> short 'n')
      <*> option (T.pack <$> str) (metavar "TERMS" <> long "query" <> short 'q')
      <*> some (argument (LocalFile <$> str) (metavar "FILE" <> help "TREC input file"))
compression = Just GZip

main :: IO ()
main = do
    let indexPath = "index"
    Right tfIdx <- BTree.open (indexPath </> "term-freqs")
        :: IO (Either String (BTree.LookupTree Term TermFrequency))
    collLength <- decode <$> BS.L.readFile (indexPath </> "coll-length") :: IO Int
    let smoothing = Dirichlet 2500 ((\n -> (n + 0.5) / (realToFrac collLength + 1)) . maybe 0 getTermFrequency . BTree.lookup tfIdx)

    (resultCount, query, dsrcs) <- execParser $ info (helper <*> opts) mempty
    let normTerms :: [(T.Text, p)] -> [(Term, p)]
        normTerms = map (first Term.fromText) . filterTerms . caseNorm
          where
            filterTerms = filter ((>2) . T.length . fst)
            caseNorm = map (first $ T.filter isAlpha . T.toCaseFold)

    let queryTerms :: M.Map Term Int
        queryTerms = M.unionsWith (+) [ M.singleton (Term.fromText t) 1
                                                 | t <- T.words query
                                                 ]

    let docs :: Producer TREC.Document (SafeT IO) ()
        docs =
            mapM_ (trecDocuments' . P.T.decodeUtf8 . decompress compression . produce) dsrcs

    runSafeT $ do
        results <-
                foldProducer (Foldl.generalize $ topK resultCount)
             $  docs
            >-> cat'                                          @TREC.Document
            >-> P.P.map (\d -> (DocName $ BS.S.toShort $ T.E.encodeUtf8 $ TREC.docNo d, TREC.docText d))
            >-> cat'                                          @(DocumentName, T.Text)
            >-> P.P.map (fmap tokeniseWithPositions)
            >-> cat'                                          @(DocumentName, [(T.Text, Position)])
            >-> P.P.map (fmap normTerms)
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> P.P.filter (any (`M.member` queryTerms) . map fst . snd)
            >-> P.P.map (second $ \terms ->
                           let docLength = DocLength $ length terms
                               terms' = map (\(term,_) -> (term, 1)) terms
                           in queryLikelihood smoothing (M.assocs queryTerms) docLength terms'
                        )
            >-> P.P.map swap
            >-> cat'                                          @(Score, DocumentName)

        liftIO $ putStrLn $ unlines $ map show results
