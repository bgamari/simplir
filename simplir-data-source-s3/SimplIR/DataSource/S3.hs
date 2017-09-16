{-# LANGUAGE OverloadedStrings #-}

module SimplIR.DataSource.S3 where

import qualified Data.Text as T
import Pipes.Safe
import qualified Pipes.Aws.S3 as P.S3
import SimplIR.DataSource.Internal

s3File :: MonadSafe m => DataSourceParser m
s3File = DataSourceParser parse
  where
    parse t
      | Just rest <- "s3://" `T.stripPrefix` t
      = let (bucket, obj) = T.break (=='/') rest
        in Just $ DataSource t $ run (P.S3.Bucket bucket) (P.S3.Object $ T.tail obj)
      | otherwise
      = Nothing
      where
        run bucket obj = P.S3.fromS3WithRetries retryPolicy bucket obj
          where retryPolicy = P.S3.warnOnRetry $ P.S3.retryNTimes 10
