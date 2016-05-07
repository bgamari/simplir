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

import Pipes
import qualified Pipes.Prelude as PP

import Utils
import DiskIndex
import CollectPostings
import Types
import RetrievalModels.QueryLikelihood
import TopK

main :: IO ()
main = do
    idx <- DiskIndex.open "index" :: IO (DiskIndex (DocumentName, DocumentLength) [Position])

    let query = ["beer", "concert"]
        query' = map (,1) query

    let termPostings :: Monad m => [(Term, Producer (Posting [Position]) m ())]
        termPostings = map (\term -> (term, each $ fromJust $ DiskIndex.lookupPostings term idx)) query

    results <- foldProducer (Fold.generalize $ topK 20)
        $ collectPostings termPostings
       >-> PP.mapFoldable (\(docId, terms) -> (docId,,map (fmap length) terms) . snd <$> DiskIndex.lookupDoc docId idx)
       >-> PP.map (swap . queryLikelihood query')
       >-> cat'                            @(Score, DocumentId)
       >-> PP.map (second $ fst . fromJust . flip DiskIndex.lookupDoc idx)
       >-> cat'                            @(Score, DocumentName)

    putStrLn $ unlines $ map show $ results
    return ()
