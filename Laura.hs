{-# LANGUAGE OverloadedStrings #-}

import qualified Pipes.Aws.S3 as S3
import qualified Pipes.GZip as GZip
import Pipes
import qualified Pipes.Prelude as PP
import qualified Pipes.ByteString as PBS
import qualified Data.ByteString as BS
import Data.Warc as Warc
import System.IO (stdout)


bucket = "aws-publicdatasets"
object = "common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"

decompressAll :: Producer BS.ByteString IO r
              -> Producer BS.ByteString IO r
decompressAll = go
  where
    go prod = do
        res <- GZip.decompress' prod
        case res of
            Left prod' -> go prod'
            Right r    -> return r

main :: IO ()
main = do
    r <- S3.fromS3 bucket object $ \resp -> do
        let warc = Warc.parseWarc $ decompressAll $ S3.responseBody resp
        Warc.iterRecords handleRecord warc
    putStrLn "That was all. This is left over..."
    runEffect $ r >-> PBS.stdout
    return ()

handleRecord :: MonadIO m => Record m a -> m a
handleRecord r = do
    liftIO $ print $ recHeader r
    --runEffect $ Warc.recContent r >-> PBS.toHandle stdout
    runEffect $ Warc.recContent r >-> PP.drain
