{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module SimplIR.WarcDocSource
    ( readRecord
    , decodeDocuments
    ) where

import Control.Monad.Trans.Except
import Control.Monad.State.Strict
import Control.Monad.Catch
import Control.Error
import Data.Foldable
import Prelude hiding (log)

import qualified Data.SmallUtf8 as Utf8
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T.E
import qualified Data.Text.ICU.Convert as ICU

import qualified Data.CaseInsensitive as CI

import           Pipes
import qualified Pipes.Prelude as P.P
import qualified Data.Attoparsec.ByteString.Lazy as Atto
import System.Logging.Facade

import Control.Lens as Lens

import Data.Warc as Warc
import qualified Network.HTTP.Types as Http
import qualified Network.HTTP.Parse as Http
import Network.HTTP.Media
import SimplIR.Types

data MsgType = MsgResponse | MsgRequest
             deriving (Show, Eq, Ord, Bounded, Enum)

readRecord :: Monad m
           => RecordHeader
           -> Producer BS.ByteString m b
           -> Producer (RecordHeader, BS.L.ByteString) m b
readRecord hdr prod = do
    (body, rest) <- lift $ P.P.toListM' prod
    yield (hdr, BS.L.fromChunks body)
    return rest

decodeDocuments :: MonadIO m
                => Pipe (RecordHeader, BS.L.ByteString) (DocumentName, T.Text) m r
decodeDocuments =
    mapFoldableM (runExceptT . decodeDocument)
  where
    logError (docName, Right x)                  = return $ Just x
    logError (docName, Left (LogMesg level msg)) = do log level $ "failed: "++msg
                                                      return Nothing

decodeDocument :: MonadIO m
               => (RecordHeader, BS.L.ByteString)
               -> ExceptT LogMesg m (DocumentName, T.Text)
decodeDocument (hdr, content) = do
    msgType <- failWith (LogMesg DEBUG "failed to find message type")
             $ recordHttpMsgType hdr

    when (msgType /= MsgResponse) $ throwE (LogMesg DEBUG "not a response")

    recId   <- failWith (LogMesg WARN "failed to find record id")
             $ hdr ^? recHeaders . Lens.each . Warc._WarcRecordId

    (respHeaders, body)
            <- case Atto.parse Http.response content of
                   Atto.Done rest respHeaders -> return (respHeaders, rest)
                   Atto.Fail err _ _          ->
                       throwE $ LogMesg WARN $ "failed to parse response HTTP headers: "++ show err

    decode  <- findDecoder respHeaders

    let Warc.RecordId (Warc.Uri docUri) = recId
        !docName = DocName $ Utf8.fromAscii docUri
    return (docName, decode $ BS.L.toStrict body)

mapFoldableM :: (Monad m, Foldable f)  => (a -> m (f b)) -> Pipe a b m r
mapFoldableM f = P.P.mapM f >-> P.P.concat

recordHttpMsgType :: RecordHeader -> Maybe MsgType
recordHttpMsgType hdr = do
    ctypeStr <- hdr ^? Warc.recHeaders . Lens.each . Warc._ContentType
    ctype <- parseAccept ctypeStr
    guard $ ctype `matches` ("application" // "http")
    msgtype <- "msgtype" `M.lookup` parameters ctype
    case CI.mk msgtype of
        "response" -> pure MsgResponse
        "request"  -> pure MsgRequest
        a          -> mzero

data LogMesg = LogMesg !LogLevel String

decodeTextWithCharSet :: MonadIO m
                      => String  -- ^ character set name
                      -> ExceptT LogMesg m (BS.ByteString -> T.Text)
decodeTextWithCharSet "utf-8" = return T.E.decodeUtf8
decodeTextWithCharSet charset = do
    mconverter <- liftIO $ handleAll (return . Left)
                $ fmap Right $ ICU.open charset Nothing
    case mconverter of
        Right converter -> return $ ICU.toUnicode converter
        Left err        -> throwE $ LogMesg INFO $ "failed to open converter for "++show charset++": "++show err

findDecoder :: MonadIO m => Http.Response -> ExceptT LogMesg m (BS.ByteString -> T.Text)
findDecoder resp = do
    ctypeHdr   <- failWith (LogMesg WARN "failed to find Content-Type header")
                $ Http.hContentType `lookup` Http.respHeaders resp

    ctype      <- failWith (LogMesg WARN "failed to parse HTTP Content-Type")
                $ parseAccept ctypeHdr

    charset    <- failWith (LogMesg INFO "failed to find charset")
                $ "charset" `M.lookup` parameters ctype

    decode     <- decodeTextWithCharSet (BS.unpack $ CI.original charset)

    return decode
