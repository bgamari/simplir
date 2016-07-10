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
import qualified SimplIR.DataSource.Gpg as Gpg

readKbaFile :: (MonadSafe m, MonadBaseControl IO m)
            => DataSource -> m BS.L.ByteString
readKbaFile src =
    P.BS.toLazyM $ DataSource.decompress compression $ maybeDecrypt
                 $ DataSource.produce dsrcLocation
  where
    DataSource{..} = src
    maybeDecrypt
      | isEncrypted = Gpg.decrypt
      | otherwise   = id
      where
        isEncrypted = ".gpg" `T.isInfixOf` getFileName dsrcLocation
    compression
      | ".xz" `T.isInfixOf` getFileName dsrcLocation = Just Lzma
      | otherwise = Nothing
