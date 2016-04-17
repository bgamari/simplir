{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified DiskIndex
import qualified DiskIndex.Posting as PostingIdx
import Types

main :: IO ()
main = do
    idx <- DiskIndex.open "index" :: IO (DiskIndex.DiskIndex DocumentName [Position])
    mapM_ print $ PostingIdx.walk $ DiskIndex.tfIdx idx
    return ()
