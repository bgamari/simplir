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

args :: Parser (FilePath, Int, [Term])
args =
    (,,)
      <$> option str (short 'i' <> long "index" <> value "index" <> help "index path")
      <*> option auto (short 'n' <> long "count" <> value 20 <> help "result count")
      <*> some (argument (Term.fromString <$> str) (help "query terms"))

main :: IO ()
main = do
    (indexPath, resultCount, query) <- execParser $ info (helper <*> args) mempty
    idx <- DiskIndex.open "index" :: IO (DiskIndex (DocumentName, DocumentLength) [Position])

    let query' = map (,1) query
        termPostings :: Monad m => [(Term, Producer (Posting [Position]) m ())]
        termPostings = map (\term -> ( term
                                     , each $ fromMaybe [] $ DiskIndex.lookupPostings term idx)
                           ) query

    results <- foldProducer (Fold.generalize $ topK resultCount)
        $ collectPostings termPostings
       >-> PP.mapFoldable (\(docId, terms) -> (docId,,map (fmap length) terms) . snd <$> DiskIndex.lookupDoc docId idx)
       >-> PP.map (swap . queryLikelihood query')
       >-> cat'                            @(Score, DocumentId)
       >-> PP.map (second $ fst . fromMaybe (error "failed to lookup document name")
                                . flip DiskIndex.lookupDoc idx)
       >-> cat'                            @(Score, DocumentName)

    putStrLn $ unlines $ map show $ results
    return ()
