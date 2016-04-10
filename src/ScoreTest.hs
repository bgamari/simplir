{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

import Data.Maybe
import Data.Binary
import qualified Data.Map as M
import Control.Monad.Trans.Except

import Pipes
import qualified Pipes.Prelude as PP

import DiskIndex
import CollectPostings
import Types
import RetrievalModels.QueryLikelihood

main :: IO ()
main = do
    Right postings <- openIndex "postings" :: IO (Either String (DiskIndex [Position]))
    let query = ["beer", "concert"]
        query' = map (,1) query
    let termPostings :: Monad m => [(Term, Producer (Posting [Position]) m ())]
        termPostings = map (\term -> (term, each $ fromJust $ DiskIndex.lookup postings term)) query
    runEffect $  collectPostings termPostings
             >-> PP.map (queryLikelihood query')
             >-> PP.mapM_ print
    return ()
