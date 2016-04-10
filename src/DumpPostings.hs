{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

import Data.Binary
import qualified Data.Map as M
import Control.Monad.Trans.Except

import Pipes
import qualified Pipes.Prelude as PP

import qualified BTree.BinaryList as BList
import BTree.BinaryList (BinaryList)
import DiskIndex.TermFreq as DiskIndex
import Types

main :: IO ()
main = do
    Right postings <- openIndex "postings" :: IO (Either String (DiskIndex [Position]))
    mapM_ print $ walk postings

    let docIds = BList.open "docids" :: BinaryList (DocumentId, DocumentName)
    dumpBList docIds

    print $ DiskIndex.lookup postings "beer"

    return ()

dumpBList :: (Binary a, Show a) => BinaryList a -> IO ()
dumpBList blist = do
    Right stream <- runExceptT $ BList.stream blist
    res <- runEffect $ stream >-> PP.mapM_ print
    print res
