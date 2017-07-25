{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}

module SimplIR.TrecStreaming
    ( -- * Stream items
      StreamItem(..)
    , ContentItem(..)
    , readItems
    , DocumentId(..)
    , StreamId(..)
    , Url(..)
    , Version(..)
    , AnnotatorId(..)
      -- ** Raw parser
    , parseStreamItem
    ) where

import Control.Applicative
import Data.Int
import GHC.Generics
import Prelude

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.HashMap.Strict as H

import Data.Time.Clock
import Data.Time.Clock.POSIX
import Pinch
import Pinch.Protocol

newtype AnnotatorId = AnnotatorId { getAnnotatorId :: T.Text }
                    deriving (Eq, Ord, Show, Generic)

newtype DocumentId = DocumentId { getDocumentId :: T.Text }
                   deriving (Eq, Ord, Show, Generic)

data Version = V010 | V020 | V030
             deriving (Eq, Ord, Enum, Bounded, Show, Generic)

newtype Url = Url { getUrl :: T.Text }
            deriving (Eq, Ord, Show, Generic)

newtype StreamId = StreamId { getStreamId :: T.Text }
                 deriving (Eq, Ord, Show, Generic)

data StreamItem = StreamItem { version      :: !Version
                             , documentId   :: !DocumentId
                             , absoluteUrl  :: !(Maybe Url)
                             , source       :: !(Maybe T.Text)
                             , streamId     :: !StreamId
                             , streamTime   :: !UTCTime
                             , body         :: Maybe ContentItem
                             , otherContent :: H.HashMap T.Text ContentItem
                             }
                 deriving (Show)

parseStreamTime :: Value TStruct -> Parser UTCTime
parseStreamTime v =
    posixSecondsToUTCTime . realToFrac <$> (v .: 1 :: Parser Double)

parseStreamItem :: Value TStruct -> Parser StreamItem
parseStreamItem v = do
    version <- toEnum . fromIntegral <$> ((v .: 1) :: Parser Int32)
    documentId <- DocumentId <$> v .: 2
    absoluteUrl <- fmap Url <$> v .:? 3
    source <- v .:? 6
    body <- v .:? 7
    streamId <- StreamId <$> v .: 9
    streamTime <- v .: 10 >>= parseStreamTime
    otherContent <- v .: 11
    return StreamItem{..}

data ContentItem = ContentItem { encoding     :: !(Maybe T.Text)
                               , mediaType    :: !(Maybe T.Text)
                               , cleanHtml    :: !(Maybe T.Text)
                               , cleanVisible :: !(Maybe T.Text)
                               }
                 deriving (Show)

instance Pinchable ContentItem where
    type Tag ContentItem = TStruct
    unpinch v = do
        encoding <- v .:? 2
        mediaType <- v .:? 3
        cleanHtml <- v .:? 4
        cleanVisible <- v .:? 5
        return ContentItem{..}
    pinch = undefined

readItems :: BS.ByteString -> [StreamItem]
readItems = go
  where
    go bs
      | BS.null bs  = []
      | otherwise   =
        let (bs', val) = either error id $ deserializeValue' binaryProtocol bs
            item = either error id $ runParser (parseStreamItem val)
        in item : go bs'
