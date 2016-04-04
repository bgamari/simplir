{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

module WarcDocSource
    ( DocumentSink(..)
    , handleRecord
    ) where

import Control.Monad.Trans.Except
import Control.Monad.State.Strict
import Control.Monad.Catch
import Control.Error
import Data.Foldable
import Data.Monoid
import Data.Functor.Contravariant

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.ByteString.Short as BS.S
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Lazy as T.L
import qualified Data.Text.Encoding as T.E
import qualified Data.Text.ICU.Convert as ICU
import qualified Data.Vector.Unboxed as VU

import qualified Data.CaseInsensitive as CI

import           Pipes
import qualified Pipes.Prelude as P.P
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


-- | The @p@ argument represents what sits "inside" a posting (e.g. @()@ for a boolean retrieval index
-- and positional information for a positional index).
newtype DocumentSink m p = DocSink { pushDocument :: DocumentName -> p -> m () }

instance Contravariant (DocumentSink m) where
    contramap f (DocSink g) = DocSink (\docName postings -> g docName (f postings))

data MsgType = MsgResponse | MsgRequest
             deriving (Show, Eq, Ord, Bounded, Enum)

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

handleRecord :: (MonadIO m)
             => DocumentSink m (M.Map Term (VU.Vector Position))
             -> Record m r
             -> m r
handleRecord docSink r@(Record {..}) = do
    (res, rest) <- P.Parse.runStateT (handleRecord' docSink r) recContent
    case res of
        Left err -> liftIO $ putStrLn $ "failed:"++err
        Right () -> return ()
    runEffect $ rest >-> P.P.drain

handleRecord' :: (MonadIO m)
              => DocumentSink m (M.Map Term (VU.Vector Position))
              -> Record m r
              -> Parser BS.ByteString m (Either String ())
handleRecord' docSink r@(Record {..}) = runExceptT $ do
    msgType <- failWith "failed to find message type"
             $ recordHttpMsgType r
    when (msgType /= MsgResponse) $ throwE "not a response"

    recId   <- failWith "failed to find record id"
             $ recHeader ^? Lens.each . Warc._WarcRecordId

    liftIO $ print recId
    Clean.HtmlDocument {..} <- ExceptT handleRecordBody
    let tokens :: [(Term, Position)]
        tokens = tokeniseWithPositions $ {-# SCC "rawContent" #-}T.L.toStrict $ docTitle <> "\n" <> docBody

        accumd :: M.Map Term (VU.Vector Position)
        !accumd = foldTokens accumPositions tokens

        Warc.RecordId (Warc.Uri docUri) = recId
        !docName = DocName $ BS.S.toShort docUri
    lift $ lift $ pushDocument docSink docName accumd

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
    respEither <- failWithM "unexpected end of response body"
                $ P.Atto.parse Http.response

    resp       <- fmapLT (\err -> "failed to parse response HTTP headers: "++ show err)
                $ hoistEither respEither

    ctypeHdr   <- failWith "failed to find Content-Type header"
                $ Http.hContentType `lookup` Http.respHeaders resp

    ctype      <- failWith "failed to parse HTTP Content-Type"
                $ parseAccept ctypeHdr

    charset    <- failWith "failed to find charset"
                $ "charset" `M.lookup` parameters ctype

    decode     <- decodeTextWithCharSet (BS.unpack $ CI.original charset)

    content    <- decode . BS.L.toStrict . BS.L.fromChunks <$> lift P.Parse.drawAll

    return $ {-# SCC "clean" #-} Clean.clean content
