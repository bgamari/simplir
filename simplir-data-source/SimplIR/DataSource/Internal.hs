{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

module SimplIR.DataSource.Internal
    ( -- * Types
      DataSource(..)
    , DataSourceParser(..)
    , dataLocationReadM
      -- * Sources
    , localFile
    ) where

import           Data.Semigroup
import           Data.Function (on)
import           System.IO

import           Data.ByteString (ByteString)
import qualified Data.Text as T
import           Options.Applicative

import           Pipes
import           Pipes.Safe
import qualified Pipes.ByteString as P.BS

-- | A 'DataSource' describes the location of some raw data (e.g. a file
-- on the filesystem or S3).
data DataSource m
    = DataSource { dataSourceUrl :: T.Text -- ^ some URLish representation of the resource
                 , runDataSource :: MonadIO m => Producer ByteString m ()
                 }
instance Eq (DataSource m) where
    (==) = (==) `on` dataSourceUrl

instance Ord (DataSource m) where
    compare = compare `on` dataSourceUrl

instance Show (DataSource m) where
    show x = "DataSource("++T.unpack (dataSourceUrl x)++")"

newtype DataSourceParser m = DataSourceParser { parseDataSource :: T.Text -> Maybe (DataSource m) }

instance Semigroup (DataSourceParser m) where
    DataSourceParser a <> DataSourceParser b =
        DataSourceParser $ \l -> a l <|> b l

instance Monoid (DataSourceParser m) where
    mempty = DataSourceParser $ const Nothing
    mappend = (<>)

localFile :: MonadSafe m => DataSourceParser m
localFile = DataSourceParser parse
  where
    parse t
      | Just rest <- "file://" `T.stripPrefix` t
      = Just $ DataSource t (run $ T.unpack rest)

      | otherwise
      = Just $ DataSource ("file://"<>t) (run $ T.unpack t)

    run path =
        bracket (liftIO $ openFile path ReadMode) (liftIO . hClose) P.BS.fromHandle

dataLocationReadM :: DataSourceParser m -> ReadM (DataSource m)
dataLocationReadM p = do
    x <- str
    maybe (fail $ "Failed to parse data source: "++x) return $ parseDataSource p $ T.pack x
