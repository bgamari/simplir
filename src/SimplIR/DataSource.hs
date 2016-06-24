{-# LANGUAGE OverloadedStrings #-}

module SimplIR.DataSource
   (
     -- * Data sources
     DataSource(..)
   , parseDataSource
   , dataSource

     -- * Data locations
   , DataLocation(..)
   , parseDataLocation
   , getFileName
   , getFilePath
   , produce

     -- * Compression
   , Compression(..)
   , guessCompression
   , decompress
   , withCompressedSource
   ) where

import           Data.Monoid
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

-- | A 'DataLocation' describes the location of some raw data (e.g. a file
-- on the filesystem or S3).
data DataLocation = LocalFile { filePath :: FilePath }
                  | S3Object { s3Bucket :: P.S3.Bucket
                             , s3Object :: P.S3.Object
                             }
                  deriving (Show, Ord, Eq)

-- | Get some sensible approximation of a file name for the given
-- 'DataLocation'.
getFileName :: DataLocation -> T.Text
getFileName (LocalFile path) = T.pack $ takeFileName path
getFileName (S3Object _ (P.S3.Object obj)) = obj

-- | Get some sensible approximation of a "path" for the given
-- 'DataLocation'.
getFilePath :: DataLocation -> T.Text
getFilePath (LocalFile path) = T.pack path
getFilePath (S3Object (P.S3.Bucket bucket) (P.S3.Object obj)) = "s3://" <> bucket <> "/" <> obj

-- | Parse a 'DataLocation' from its string representation.
parseDataLocation :: T.Text -> Maybe DataLocation
parseDataLocation t
  | Just rest <- "s3://" `T.stripPrefix` t
  = let (bucket, obj) = T.break (=='/') rest
    in Just $ S3Object (P.S3.Bucket bucket) (P.S3.Object $ T.tail obj)

  | Just rest <- "file://" `T.stripPrefix` t
  = Just $ LocalFile $ T.unpack rest

  | otherwise
  = Just $ LocalFile $ T.unpack t

-- | Produce the raw data from a 'DataLocation'
produce :: (MonadSafe m)
        => DataLocation
        -> Producer ByteString m ()
produce (LocalFile path) =
    bracket (liftIO $ openFile path ReadMode) (liftIO . hClose) P.BS.fromHandle
produce (S3Object bucket object) =
    P.S3.fromS3 bucket object

-- | A compression method
data Compression = GZip   -- ^ e.g. @file.gz@
                 | Lzma   -- ^ e.g. @file.xz@
                 deriving (Show, Ord, Eq)

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

-- | A 'DataSource' describes the location of some bit of data, as well any
-- decompression it may need
data DataSource = DataSource { dsrcCompression :: Maybe Compression
                             , dsrcLocation    :: DataLocation
                             }
                deriving (Show, Ord, Eq)

-- | Produce the data from a 'DataSource'
dataSource :: MonadSafe m => DataSource -> Producer ByteString m ()
dataSource (DataSource compression location) =
    decompress compression $ produce location

-- | Parse a 'DataSource' from its textual representation, attempting to
-- identify any compression from the file name.
parseDataSource :: T.Text             -- ^ location
                -> Maybe DataSource
parseDataSource t = do
    loc <- parseDataLocation t
    let compression = guessCompression (getFileName loc)
    return $ DataSource compression loc

-- | Try to identify the compression method of a file.
guessCompression :: T.Text            -- ^ file name
                 -> Maybe Compression
guessCompression name
  | ".gz" `T.isSuffixOf` name  = Just GZip
  | ".xz" `T.isSuffixOf` name  = Just Lzma
  | ".bz2" `T.isSuffixOf` name = error "parseDataSource: bzip2 not implemented"
  | otherwise                  = Nothing
