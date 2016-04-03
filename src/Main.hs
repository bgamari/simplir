{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}

import Control.Exception (evaluate, try)
import Control.Monad (guard, mzero, when)
import Control.Monad.Trans.Except
import Control.Monad.State
import Control.Monad.Catch
import Control.Error
import Data.Foldable
import Data.Monoid
import System.IO (stdout)

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.ByteString.Short as BS.S
import qualified Data.DList as DList
import           Data.DList (DList)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Lazy as T.L
import qualified Data.Text.Encoding as T.E
import qualified Data.Text.ICU.Convert as ICU
import qualified Data.Vector.Unboxed as VU

import qualified Data.CaseInsensitive as CI

import Control.Monad.Morph
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
import Types

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
    P.S3.fromS3 bucket object $ \resp -> do
        let warc = Warc.parseWarc
                 $ decompressAll
                 $ hoist liftIO
                 $ P.S3.responseBody resp
        postings <- flip execStateT mempty $ do
            r <- Warc.iterRecords handleRecord warc
            liftIO $ putStrLn "That was all. This is left over..."
            runEffect $ r >-> P.BS.stdout
        liftIO $ print postings

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

handleRecord :: ( MonadIO m
                , MonadState (DList (DocumentName, [(Term, VU.Vector Position)] )) m)
             => Record m r -> m r
handleRecord r@(Record {..}) = do
    let msgtype = recordHttpMsgType r
    rest <- case msgtype of
        Just MsgResponse -> do
            Just recId <- pure $ recHeader ^? Lens.each . Warc._WarcRecordId
            liftIO $ print recId
            (docEither, rest) <- P.Parse.runStateT handleRecordBody recContent
            case docEither of
                Right Clean.HtmlDocument {..} -> do
                    let tokens :: [(Term, Position)]
                        tokens = tokeniseWithPositions $ T.L.toStrict $ docTitle <> "\n" <> docBody
                        accumd :: M.Map Term (VU.Vector Position)
                        accumd = foldTokens accumPositions tokens

                        Warc.RecordId (Warc.Uri docName) = recId

                    modify (`DList.snoc` (DocName $ BS.S.toShort docName, M.assocs accumd))
            return rest
        _ -> return recContent
    runEffect $ rest >-> P.P.drain

decodeTextWithCharSet :: MonadIO m
                      => String  -- ^ character set name
                      -> ExceptT String m (BS.ByteString -> T.Text)
decodeTextWithCharSet "utf-8" = return T.E.decodeUtf8
decodeTextWithCharSet charset = do
    mconverter <- liftIO $ handleAll (return . Left)
                $ fmap Right $ ICU.open charset Nothing
    case mconverter of
        Right converter -> return $ ICU.toUnicode converter
        Left err        -> throwE $ "failed to open converter for "++show charset++": "++show err

handleRecordBody :: (MonadIO m) => Parser BS.ByteString m (Either String HtmlDocument)
handleRecordBody = runExceptT $ do
    respEither <- failWithM "unexpected end of response"
                $ P.Atto.parse Http.response

    resp       <- fmapLT (\err -> "failed to parse HTTP headers: "++ show err)
                $ hoistEither respEither

    ctypeHdr   <- failWith "failed to find Content-Type header"
                $ Http.hContentType `lookup` Http.respHeaders resp

    ctype      <- failWith "failed to parse HTTP Content-Type"
                $ parseAccept ctypeHdr

    charset    <- failWith "failed to find charset"
                $ "charset" `M.lookup` parameters ctype

    decode     <- decodeTextWithCharSet (BS.unpack $ CI.original charset)

    content    <- decode . BS.L.toStrict . BS.L.fromChunks <$> lift P.Parse.drawAll

    return $ Clean.clean content
