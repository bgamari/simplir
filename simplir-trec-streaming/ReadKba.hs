{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}

module ReadKba where

import Control.Monad.Trans.Control
import Pipes.Safe
import qualified Data.ByteString.Lazy as BS.L
import qualified Pipes.ByteString as P.BS
import qualified Data.Text as T
import SimplIR.DataSource as DataSource
import SimplIR.DataSource.Compression
import qualified SimplIR.DataSource.Gpg as Gpg

readKbaFile :: (MonadSafe m, MonadBaseControl IO m)
            => DataSource m -> m BS.L.ByteString
readKbaFile src =
    P.BS.toLazyM $ decompressed $ maybeDecrypt
                 $ DataSource.runDataSource src
  where
    maybeDecrypt
      | isEncrypted = Gpg.decrypt
      | otherwise   = id
      where
        isEncrypted = ".gpg" `T.isInfixOf` dataSourceUrl src
