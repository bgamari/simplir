{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

import Control.Monad (guard, mzero, when)
import Control.Monad.Trans.Except
import Control.Error
import System.IO (stdout)

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Map as M

import qualified Data.CaseInsensitive as CI

import           Pipes
import qualified Pipes.Aws.S3 as P.S3
import qualified Pipes.GZip as P.GZip
import qualified Pipes.Prelude as P.P
import qualified Pipes.ByteString as P.BS
import qualified Pipes.Parse as P.Parse
import           Pipes.Parse (Parser)
import qualified Pipes.Attoparsec as P.Atto

import Control.Lens as Lens

import Data.Warc as Warc
import Data.Warc.Header as Warc
import qualified Network.HTTP.Types as Http
import qualified HTTP.Parse as Http
import Network.HTTP.Media

bucket = "aws-publicdatasets"
object = "common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"

decompressAll :: Producer BS.ByteString IO r
              -> Producer BS.ByteString IO r
decompressAll = go
  where
    go prod = do
        res <- P.GZip.decompress' prod
        case res of
            Left prod' -> go prod'
            Right r    -> return r

main :: IO ()
main = do
    r <- P.S3.fromS3 bucket object $ \resp -> do
        let warc = Warc.parseWarc $ decompressAll $ P.S3.responseBody resp
        Warc.iterRecords handleRecord warc
    putStrLn "That was all. This is left over..."
    runEffect $ r >-> P.BS.stdout
    return ()

data MsgType = MsgResponse | MsgRequest
             deriving (Show)

recordHttpMsgType :: Record m r -> Maybe MsgType
recordHttpMsgType (Record {..}) = do
    ctypeStr <- recHeader ^? Lens.each . Warc._ContentType
    ctype <- parseAccept ctypeStr
    guard $ ctype `matches` ("application" // "http")
    msgtype <- "msgtype" `M.lookup` parameters ctype
    case CI.mk msgtype of
        "response" -> pure MsgResponse
        "request"  -> pure MsgRequest
        a          -> mzero

handleRecord :: MonadIO m => Record m r -> m r
handleRecord r@(Record {..}) = do
    liftIO $ print recHeader
    let msgtype = recordHttpMsgType r
    liftIO $ print msgtype
    rest <- case msgtype of
        Just MsgResponse -> do
            (postings, rest) <- P.Parse.runStateT handleResponseRecord recContent
            liftIO $ print postings
            return rest
        _ -> return recContent
    runEffect $ rest >-> P.P.drain

type Postings = [BS.ByteString]

handleResponseRecord :: MonadIO m => Parser BS.ByteString m (Either String Postings)
handleResponseRecord = runExceptT $ do
    Right resp <- failWithM "failed to parse HTTP headers"
                $ P.Atto.parse Http.response

    ctypeHdr   <- failWith "failed to find Content-Type header"
                $ Http.hContentType `lookup` Http.respHeaders resp

    ctype      <- failWith "failed to parse HTTP Content-Type"
                $ parseAccept ctypeHdr

    charset    <- failWith "failed to find charset"
                $ "charset" `M.lookup` parameters ctype

    failWith ("unknown character set: "++show charset)
                $ guard (charset /= "utf-8")

    content <- lift P.Parse.drawAll
    return content
