{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

module SimplIR.DataSource
    ( -- * Types
      DataSource
    , DataSourceParser(..)
    , runDataSource
    , dataSourceFileName
    , dataSourceUrl
    , dataLocationReadM
      -- * Sources
    , localFile
    ) where

import qualified Data.Text as T
import System.FilePath
import SimplIR.DataSource.Internal

dataSourceFileName :: DataSource m -> FilePath
dataSourceFileName = takeFileName . T.unpack . dataSourceUrl
