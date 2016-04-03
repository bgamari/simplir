{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

import Control.Monad (guard, mzero, when)
import Control.Monad.Trans.Except
import Control.Error
import Data.Monoid
import System.IO (stdout)

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Text as T
import qualified Data.Text.Lazy as T.L
import qualified Data.Text.Encoding as T.E
import qualified Data.Text.ICU.Convert as ICU
import qualified Data.Map.Strict as M

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
import Text.HTML.Clean as Clean
import Tokenise

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
    let msgtype = recordHttpMsgType r
    liftIO $ print msgtype
    rest <- case msgtype of
        Just MsgResponse -> do
            (doc, rest) <- P.Parse.runStateT handleResponseRecord recContent
            let Right Clean.HtmlDocument {..} = doc
                tokens = tokenise $ T.L.toStrict $ docTitle <> "\n" <> docBody
            return rest
        _ -> return recContent
    runEffect $ rest >-> P.P.drain

decodeTextWithCharSet :: MonadIO m
                      => String  -- ^ character set name
                      -> ExceptT String m (BS.ByteString -> T.Text)
decodeTextWithCharSet "utf-8" = return T.E.decodeUtf8
decodeTextWithCharSet charset = do
    converter <- fmapLT (\err -> "failed to open converter for "++show charset++": "++show err)
               $ tryIO $ liftIO $ ICU.open charset Nothing
    return $ ICU.toUnicode converter

handleResponseRecord :: MonadIO m => Parser BS.ByteString m (Either String HtmlDocument)
handleResponseRecord = runExceptT $ do
    Right resp <- failWithM "failed to parse HTTP headers"
                $ P.Atto.parse Http.response

    ctypeHdr   <- failWith "failed to find Content-Type header"
                $ Http.hContentType `lookup` Http.respHeaders resp

    ctype      <- failWith "failed to parse HTTP Content-Type"
                $ parseAccept ctypeHdr

    charset    <- failWith "failed to find charset"
                $ "charset" `M.lookup` parameters ctype

    decode     <- decodeTextWithCharSet (BS.unpack $ CI.original charset)

    content    <- decode . BS.L.toStrict . BS.L.fromChunks <$> lift P.Parse.drawAll
    return $ Clean.clean content
