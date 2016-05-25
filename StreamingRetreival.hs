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
    (resultCount, query, dsrcs) <- execParser $ info (helper <*> opts) mempty
    let normTerms :: [(T.Text, p)] -> [(Term, p)]
        normTerms = map (first Term.fromText) . caseNorm
          where
            caseNorm = map (first $ T.toCaseFold)

    let queryTerms :: [(Term, Int)]
        queryTerms = M.assocs $ M.unionsWith (+) [ M.singleton (Term.fromText t) 1
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
            >-> P.P.map (\(docName, terms) ->
                           let docLength = DocLength $ length terms
                               terms' = map (\(term,_) -> (term, 1)) terms
                           in swap $ queryLikelihood NoSmoothing queryTerms (docName, docLength, terms')
                        )
            >-> cat'                                          @(Score, DocumentName)

        liftIO $ putStrLn $ unlines $ map show results
