{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Data.SmallUtf8 where

import Data.String
import Data.Monoid
import Control.DeepSeq
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Hashable
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as BS.S
import qualified Data.ByteString.Builder as BS.B
import qualified Data.Text as T
import qualified Data.Text.Encoding as T.E

newtype SmallUtf8 = SmallUtf8 BS.S.ShortByteString
             deriving (Eq, Ord, Show, NFData, Hashable, IsString)

fromText :: T.Text -> SmallUtf8
fromText = SmallUtf8 . BS.S.toShort . T.E.encodeUtf8

toText :: SmallUtf8 -> T.Text
toText (SmallUtf8 t) = T.E.decodeUtf8 $ BS.S.fromShort t

fromString :: String -> SmallUtf8
fromString = fromText . T.pack

toString :: SmallUtf8 -> String
toString = T.unpack . toText

fromAscii :: BS.ByteString -> SmallUtf8
fromAscii = SmallUtf8 . BS.S.toShort

instance Binary SmallUtf8 where
    get = do
        len <- fromIntegral <$> getWord8
        SmallUtf8 . BS.S.toShort <$> getByteString len
    {-# INLINE get #-}
    put (SmallUtf8 t) = do
        putWord8 $ fromIntegral $ BS.S.length t
        putBuilder $ BS.B.shortByteString t
    {-# INLINE put #-}

instance Aeson.ToJSON SmallUtf8 where
    toJSON = Aeson.String . toText
    toEncoding (SmallUtf8 x) = Aeson.unsafeToEncoding $
        BS.B.char7 '"' <> BS.B.shortByteString x <> BS.B.char7 '"'
    {-# INLINE toEncoding #-}

instance Aeson.FromJSON SmallUtf8 where
    parseJSON (Aeson.String t) = pure $ fromText t
    parseJSON _                = fail "ToJson(SmallUtf8): Expected String"
