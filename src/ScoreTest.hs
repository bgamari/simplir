{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

import Data.Maybe
import Data.Tuple
import Data.Binary
import qualified Data.Map as M
import Control.Monad.Trans.Except
import qualified Control.Foldl as Fold

import Pipes
import qualified Pipes.Prelude as PP

import Utils
import DiskIndex.TermFreq as PostingIdx
import DiskIndex.Document as DocIdx
import CollectPostings
import Types
import RetrievalModels.QueryLikelihood
import TopK

main :: IO ()
main = do
    Right postings <- PostingIdx.open "postings" :: IO (Either String (DiskIndex [Position]))
    docIndex <- DocIdx.open "documents.idx" :: IO (DocIndex (DocumentName, DocumentLength))

    let query = ["beer", "concert"]
        query' = map (,1) query

    let termPostings :: Monad m => [(Term, Producer (Posting [Position]) m ())]
        termPostings = map (\term -> (term, each $ fromJust $ PostingIdx.lookup postings term)) query

    results <- foldProducer (Fold.generalize $ topK 20)
        $ collectPostings termPostings
       >-> PP.mapFoldable (\(docId, terms) -> (docId,,map (fmap length) terms) . snd <$> DocIdx.lookupDoc docId docIndex)
       >-> PP.map (swap . queryLikelihood query')
       >-> cat'                            @(Score, DocumentId)

    putStrLn $ unlines $ map show $ results
    return ()
