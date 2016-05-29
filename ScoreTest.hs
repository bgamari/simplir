{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

import Data.Maybe
import Data.Tuple
import Data.Binary
import Data.Bifunctor
import qualified Data.Map as M
import Control.Monad.Trans.Except
import qualified Control.Foldl as Fold
import qualified Data.Text as T
import qualified Data.ByteString.Lazy as BS.L
import System.FilePath

import Pipes
import qualified Pipes.Prelude as PP

import Utils
import DiskIndex
import CollectPostings
import Types
import Term
import RetrievalModels.QueryLikelihood
import TopK
import Options.Applicative
import qualified BTree

args :: Parser (FilePath, Int, [Term])
args =
    (,,)
      <$> option str (short 'i' <> long "index" <> value "index" <> help "index path")
      <*> option auto (short 'n' <> long "count" <> value 20 <> help "result count")
      <*> some (argument (Term.fromString <$> str) (help "query terms"))

--smoothing = Dirichlet 2500 (const 0.01)
smoothing = Laplace

main :: IO ()
main = do
    (indexPath, resultCount, query) <- execParser $ info (helper <*> args) mempty
    idx <- DiskIndex.open indexPath :: IO (DiskIndex (DocumentName, DocumentLength) [Position])
    Right tfIdx <- BTree.open (indexPath </> "term-freqs")
        :: IO (Either String (BTree.LookupTree Term TermFrequency))

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

    putStrLn $ unlines $ map show $ results
    return ()

traceP :: (MonadIO m) => (a -> String) -> Pipe a a m r
traceP f = PP.mapM (\x -> liftIO (putStrLn $ f x) >> return x)
