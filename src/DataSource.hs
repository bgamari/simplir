module DataSource
   (
     -- * Data sources
     DataLocation(..)
   , withDataSource
     -- * Compression
   , Compression(..)
   , decompress
   , withCompressedSource
   ) where

import qualified Data.Text as T
import           Data.ByteString (ByteString)
import           System.IO

import           Pipes
import qualified Pipes.GZip as P.GZip
import qualified Pipes.Aws.S3 as P.S3
import qualified Pipes.ByteString as P.BS

data DataLocation = LocalFile { filePath :: FilePath }
                  | S3Object { s3Bucket :: P.S3.Bucket
                             , s3Object :: P.S3.Object
                             }

withDataSource :: DataLocation
               -> (Producer ByteString IO () -> IO a)
               -> IO a
withDataSource (LocalFile path) action =
    withFile path ReadMode $ action . P.BS.fromHandle
withDataSource (S3Object bucket object) action =
    P.S3.fromS3 bucket object $ \resp -> action (hoist liftIO (P.S3.responseBody resp))

data Compression = GZip

decompress :: MonadIO m
           => Maybe Compression
           -> Producer ByteString m a -> Producer ByteString m a
decompress Nothing     = id
decompress (Just GZip) = decompressGZip

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

withCompressedSource :: DataLocation -> Maybe Compression
                     -> (Producer ByteString IO () -> IO a)
                     -> IO a
withCompressedSource loc compr action =
    withDataSource loc $ action . decompress compr
