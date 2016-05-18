{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Monad
import qualified DiskIndex
import qualified DiskIndex.Posting as PostingIdx
import Types

main :: IO ()
main = do
    idx <- DiskIndex.open "index" :: IO (DiskIndex.DiskIndex DocumentName [Position])
    forM_ (PostingIdx.walk $ DiskIndex.tfIdx idx) $ \(term, postings) ->
        putStrLn $ show term ++ "\t" ++ show (map postingDocId postings)
    return ()
