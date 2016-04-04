{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

import Control.Monad.Trans.Except
import Control.Monad.State.Strict
import Data.Foldable
import Data.Monoid

import qualified Data.ByteString.Char8 as BS

import Control.Monad.Morph
import           Pipes
import qualified Pipes.Aws.S3 as P.S3
import qualified Pipes.GZip as P.GZip
import qualified Pipes.ByteString as P.BS

import Data.Warc as Warc
import qualified Network.HTTP.Types as Http

import Progress
import WarcDocSource
import AccumPostings

bucket = "aws-publicdatasets"
object = "common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"

decompressAll :: MonadIO m
              => Producer BS.ByteString m r
              -> Producer BS.ByteString m r
decompressAll = go
  where
    go prod = do
        res <- P.GZip.decompress' prod
        case res of
            Left prod' -> go prod'
            Right r    -> return r

main :: IO ()
main = do
    compProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    expProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    pollProgress compProg 1 $ \(Sum n) -> "Compressed "++show (realToFrac n / 1024 / 1024)
    pollProgress expProg 1 $ \(Sum n) -> "Expanded "++show (realToFrac n / 1024 / 1024)

    P.S3.fromS3 bucket object $ \resp -> do
        let warc = Warc.parseWarc
                 $ (>-> progressPipe expProg (Sum . BS.length))
                 $ decompressAll
                 $ hoist liftIO
                 $ (>-> progressPipe compProg (Sum . BS.length))
                 $ P.S3.responseBody resp

        ((), postings) <- runAccumPostingsSink $ do
            r <- Warc.iterRecords (handleRecord accumPostingsSink) warc
            liftIO $ putStrLn "That was all. This is left over..."
            runEffect $ r >-> P.BS.stdout
        liftIO $ print postings

