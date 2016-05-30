{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

import Data.Maybe
import Data.Tuple
import Data.Binary
import Data.Bifunctor
import qualified Data.Map as M
import qualified Data.Set as S
import Control.Monad.Trans.Except
import qualified Control.Foldl as Fold
import qualified Data.Text as T
import qualified Data.Text.IO as T.IO
import qualified Data.ByteString.Lazy as BS.L
import System.FilePath

import Pipes
import qualified Pipes.Prelude as PP

import Numeric.Log
import qualified Data.SmallUtf8 as Utf8
import Utils
import DiskIndex
import CollectPostings
import Types
import Term
import RetrievalModels.QueryLikelihood
import TopK
import Options.Applicative
import qualified BTree

type QueryId = String

args :: Parser (FilePath, Int, QueryId, [Term])
args =
    (,,,)
      <$> option str (short 'i' <> long "index" <> value "index" <> help "index path")
      <*> option auto (short 'n' <> long "count" <> value 20 <> help "result count")
      <*> option str (metavar "QUERY_ID" <> long "qid" <> short 'i' <> value "1")
      <*> some (argument (Term.fromString <$> str) (help "query terms"))

main :: IO ()
main = do
    (indexPath, resultCount, qid, query) <- execParser $ info (helper <*> args) mempty
    idx <- DiskIndex.open indexPath :: IO (DiskIndex (DocumentName, DocumentLength) [Position])
    Right tfIdx <- BTree.open (indexPath </> "term-freqs")
        :: IO (Either String (BTree.LookupTree Term TermFrequency))
    collLength <- decode <$> BS.L.readFile (indexPath </> "coll-length") :: IO Int
    let smoothing = Dirichlet 1500 ((\n -> (n + 0.5) / (realToFrac collLength + 1)) . maybe 0 getTermFrequency . BTree.lookup tfIdx)

    stopwords <- S.fromList . map Term.fromText . T.lines <$> T.IO.readFile "inquery-stopwordlist"

    let query' = map (,1) query
        termPostings :: Monad m => [(Term, Producer (Posting [Position]) m ())]
        termPostings = map (\term -> ( term
                                     , each $ fromMaybe [] $ DiskIndex.lookupPostings term idx)
                           ) query

    results <- foldProducer (Fold.generalize $ topK resultCount)
        $ collectPostings termPostings
       >-> PP.mapFoldable (\(docId, terms) -> case DiskIndex.lookupDoc docId idx of
                                                Just (docName, docLen) -> Just (docId, docName, docLen, map (second length) terms)
                                                Nothing                -> Nothing)
       >-> cat'                            @(DocumentId, DocumentName, DocumentLength, [(Term, Int)])
       >-> PP.map (\(docId, docName, docLen, terms) -> let score = queryLikelihood smoothing query' docLen terms
                                                       in Entry score docId)
       >-> cat'                            @(Entry Score DocumentId)
       >-> PP.map (fmap $ fst . fromMaybe (error "failed to lookup document name")
                              . flip DiskIndex.lookupDoc idx)
       >-> cat'                            @(Entry Score DocumentName)

    let toRunFile rank (Entry (Exp score) (DocName docName)) = unwords
            [ qid, "Q0", Utf8.toString docName, show rank, show score, "simplir" ]
    liftIO $ putStrLn $ unlines $ zipWith toRunFile [1..] results

traceP :: (MonadIO m) => (a -> String) -> Pipe a a m r
traceP f = PP.mapM (\x -> liftIO (putStrLn $ f x) >> return x)
