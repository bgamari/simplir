{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module SimplIR.Galago
    ( -- * Basic types
      Tokenized(..)
    , DocumentId(..)
    , MetadataFieldName(..)
    , FieldName(..)
      -- * Document
    , Document(..)
      -- * Rendering to Galago WARC
    , toWarc
    ) where

import Data.Char
import Data.Monoid
import Data.String
import Data.Functor.Identity
import GHC.Generics

import qualified Data.Map.Strict as M
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BS.L
import qualified Data.ByteString.Builder as BS.B
import qualified Data.Text as T
import qualified Data.Text.Lazy as T.L
import qualified Data.Text.Encoding as T.E
import qualified Data.Text.Lazy.Encoding as T.L.E
import Data.Hashable (Hashable)

import Pipes
import qualified Pipes.ByteString as P.BS

import qualified Data.Warc as Warc

newtype DocumentId = DocumentId { unDocumentId :: T.Text }
              deriving (Show, Eq, Ord, Hashable, IsString)

data Tokenized = DoNotTokenize | DoTokenize
               deriving (Eq, Show, Enum, Bounded, Generic)

data Document = Document { docId       :: DocumentId
                           -- | the 'Int' is a replication count to allow more efficient weighting.
                         , docFields   :: M.Map FieldName [(T.L.Text, Tokenized)]
                         , docMetadata :: M.Map MetadataFieldName BS.L.ByteString
                         }
              deriving (Show, Generic)

newtype MetadataFieldName = MetadataFieldName { unMetadataFieldName :: T.Text }
                          deriving (Show, Eq, Ord, Hashable, IsString)


newtype FieldName = FieldName { unFieldName :: T.Text }
            deriving (Show, Eq, Ord, Hashable, IsString)

test :: [Document]
test = [ Document { docId = "hello"
                  , docFields = M.fromList [ ("field1", [("hello", DoTokenize)])
                                           , ("field2", [("turtle", DoNotTokenize)])
                                           ]
                  , docMetadata = M.fromList [("meta1", "world")]
                  }
       , Document { docId = "hello2"
                  , docFields = M.fromList [ ("field1", [("cucumber", DoTokenize)])
                                           , ("field2", [("sea turtle", DoNotTokenize)])
                                           ]
                  , docMetadata = M.fromList [("meta1", "shirt")]
                  }
       ]

toWarc :: [Document] -> BS.L.ByteString
toWarc docs = P.BS.toLazy $ do
    Warc.encodeRecord
        $ Warc.Record { recHeader  = Warc.addField Warc.contentLength 0
                                     $ Warc.RecordHeader Warc.warc0_16 mempty
                      , recContent = return ()
                      }
    mapM_ (Warc.encodeRecord . toRecord) docs

toRecord :: Document -> Warc.Record Identity ()
toRecord (Document{..}) =
    Warc.Record { recHeader = addHeaders $ Warc.RecordHeader Warc.warc0_16 mempty
                , recContent = P.BS.fromLazy $ content
                }
  where
    addHeaders hdr =
        Warc.addField (Warc.rawField "WARC-TREC-ID") (T.L.E.encodeUtf8 $ T.L.fromStrict $ unDocumentId docId)
      $ Warc.addField Warc.contentLength (fromIntegral $ BS.L.length content)
      $ foldl addField hdr (M.toList docMetadata)
    addField acc (MetadataFieldName k, v) =
        Warc.addField (Warc.rawField $ Warc.FieldName k) v acc

    content :: BS.L.ByteString
    content = BS.B.toLazyByteString $ "<text>\n" <> foldMap toField (M.toList docFields) <> "</text>\n"

    toField :: (FieldName, [(T.L.Text, Tokenized)]) -> BS.B.Builder
    toField (fName, values) = foldMap (toFieldValue fName) values

    toFieldValue :: FieldName -> (T.L.Text, Tokenized) -> BS.B.Builder
    toFieldValue (FieldName fName) (text, tokenized) =
        "<" <> T.E.encodeUtf8Builder fName <> tokenizeAttr <> ">" <>
        T.L.E.encodeUtf8Builder (T.L.filter isAscii text) <>
        "</" <> T.E.encodeUtf8Builder fName <> ">\n"
      where
        tokenizeAttr = case tokenized of
                         DoNotTokenize -> " tokenizeTagContent=false"
                         DoTokenize    -> ""

        isAscii c = ord c < 255
