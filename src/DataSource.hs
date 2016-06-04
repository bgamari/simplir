{-# LANGUAGE OverloadedStrings #-}

module DataSource
   (
     -- * Data sources
     DataLocation(..)
   , parseDataLocation
   , getFileName
   , produce
     -- * Compression
   , Compression(..)
   , decompress
   , withCompressedSource
   ) where

import           Control.Monad (join)
import           System.IO

import           Data.ByteString (ByteString)
import qualified Data.Text as T
import           System.FilePath

import           Pipes
import           Pipes.Safe
import qualified Pipes.GZip as P.GZip
import qualified Pipes.Lzma as P.Lzma
import qualified Pipes.Aws.S3 as P.S3
import qualified Pipes.ByteString as P.BS

data DataLocation = LocalFile { filePath :: FilePath }
                  | S3Object { s3Bucket :: P.S3.Bucket
                             , s3Object :: P.S3.Object
                             }
                  deriving (Show)

-- | Get some sensible approximation of a file name for the given
-- 'DataLocation'.
getFileName :: DataLocation -> T.Text
getFileName (LocalFile path) = T.pack $ takeFileName path
getFileName (S3Object _ (P.S3.Object obj)) = obj

data Compression = GZip   -- ^ e.g. @file.gz@
                 | Lzma   -- ^ e.g. @file.xz@

parseDataLocation :: T.Text -> Maybe DataLocation
parseDataLocation t
  | Just rest <- "s3://" `T.stripPrefix` t
  = let (bucket, obj) = T.break (=='/') rest
    in Just $ S3Object (P.S3.Bucket bucket) (P.S3.Object $ T.tail obj)

  | otherwise
  = Just $ LocalFile $ T.unpack t

decompress :: MonadIO m
           => Maybe Compression
           -> Producer ByteString m a -> Producer ByteString m a
decompress Nothing     = id
decompress (Just GZip) = decompressGZip
decompress (Just Lzma) = join . P.Lzma.decompress

decompressGZip :: MonadIO m
               => Producer ByteString m r
               -> Producer ByteString m r
decompressGZip = go
  where
    go prod = do
        res <- P.GZip.decompress' prod
        case res of
            Left prod' -> go prod'
            Right r    -> return r

withCompressedSource :: MonadSafe m
                     => DataLocation -> Maybe Compression
                     -> (Producer ByteString m () -> m a)
                     -> m a
withCompressedSource loc compr action =
    action $ decompress compr (produce loc)

produce :: (MonadSafe m)
        => DataLocation
        -> Producer ByteString m ()
produce (LocalFile path) =
    bracket (liftIO $ openFile path ReadMode) (liftIO . hClose) P.BS.fromHandle
produce (S3Object bucket object) =
    P.S3.fromS3 bucket object $ \resp -> P.S3.responseBody resp
