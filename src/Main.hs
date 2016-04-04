{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

import Control.Exception (evaluate, try)
import Control.Monad (guard, mzero, when)
import Control.Monad.Trans.Except
import Control.Monad.State.Strict
import Control.Monad.Catch
import Control.Error
import Data.Foldable
import Data.Monoid
import Data.Functor.Contravariant
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
import Progress

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

-- | The @p@ argument represents what sits "inside" a posting (e.g. @()@ for a boolean retrieval index
-- and positional information for a positional index).
newtype DocumentSink m p = DocSink { pushDocument :: DocumentName -> M.Map Term p -> m () }

instance Contravariant (DocumentSink m) where
    contramap f (DocSink g) = DocSink (\docName postings -> g docName (fmap f postings))

data AccumPostingsState p = APS { apsDocIds      :: !(M.Map DocumentId DocumentName)
                                , apsFreshDocIds :: ![DocumentId]
                                , apsPostings    :: !(M.Map Term (DList (Posting p)))
                                }

newtype AccumPostingsM p m a = APM (StateT (AccumPostingsState p) m a)
                             deriving (Functor, Applicative, Monad, MonadIO)

runAccumPostingsSink :: Monad m => AccumPostingsM p m a -> m (a, M.Map Term (DList (Posting p)))
runAccumPostingsSink (APM action) = do
    (r, s) <- runStateT action s0
    return (r, apsPostings s)
  where
    s0 = APS { apsDocIds = M.empty
             , apsFreshDocIds = [DocId i | i <- [0..]]
             , apsPostings = M.empty
             }

assignDocId :: Monad m => DocumentName -> AccumPostingsM p m DocumentId
assignDocId docName = APM $ do
    docId:rest <- apsFreshDocIds <$> get
    modify' $ \s -> s { apsFreshDocIds = rest
                      , apsDocIds = M.insert docId docName (apsDocIds s) }
    return docId

insertPostings :: (Monad m, Monoid p) => TermPostings p -> AccumPostingsM p m ()
insertPostings termPostings = APM $
    modify $ \s -> s { apsPostings = foldl' (\acc (term, postings) -> M.insertWith (<>) term (DList.singleton postings) acc)
                                            (apsPostings s)
                                            termPostings }

accumPostingsSink :: (Monad m, Monoid p) => DocumentSink (AccumPostingsM p m) p
accumPostingsSink = DocSink $ \docName postings -> do
    docId <- assignDocId docName
    insertPostings $ toPostings docId (M.assocs postings)


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
             => DocumentSink m (VU.Vector Position)
             -> Record m r
             -> m r
handleRecord docSink r@(Record {..}) = do
    (res, rest) <- P.Parse.runStateT (handleRecord' docSink r) recContent
    case res of
        Left err -> liftIO $ putStrLn $ "failed:"++err
        Right () -> return ()
    runEffect $ rest >-> P.P.drain

handleRecord' :: (MonadIO m)
              => DocumentSink m (VU.Vector Position)
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
        tokens = tokeniseWithPositions $ T.L.toStrict $ docTitle <> "\n" <> docBody

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

    return $ Clean.clean content
