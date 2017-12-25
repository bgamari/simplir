{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Data.SmallUtf8
    ( SmallUtf8
      -- * Conversions
      -- ** @String@
    , toString
    , fromString
      -- ** @text@ package
    , fromText
    , toText
    , toTextBuilder
      -- ** @bytestring@ package
    , toByteString
    , fromAscii
    , unsafeFromByteString
    , toShortByteString
    , unsafeFromShortByteString
    , toByteStringBuilder
    ) where

import Data.String (IsString)
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
import qualified Data.ByteString.Short.Internal as BS.S.I
import qualified Data.ByteString.Builder as BS.B
import qualified Codec.Serialise as CBOR
import qualified Codec.Serialise.Decoding as CBOR
import qualified Codec.Serialise.Encoding as CBOR
import qualified Codec.CBOR.ByteArray as CBOR.BA
import qualified Codec.CBOR.ByteArray.Sliced as CBOR.BA.Sliced
import qualified Data.Text as T
import qualified Data.Text.Array as T.A
import qualified Data.Text.Lazy.Builder as T.B
import qualified Data.Text.Internal.Builder as T.B
import qualified Data.Text.Encoding as T.E

-- | An efficient representation for UTF-8 encoded strings.
newtype SmallUtf8 = SmallUtf8 BS.S.ShortByteString
                  deriving (Eq, Ord, Show, NFData, Hashable, IsString)

fromText :: T.Text -> SmallUtf8
fromText = SmallUtf8 . BS.S.toShort . T.E.encodeUtf8

toText :: SmallUtf8 -> T.Text
toText (SmallUtf8 t) = T.E.decodeUtf8 $ BS.S.fromShort t

toTextBuilder :: SmallUtf8 -> T.B.Builder
toTextBuilder (SmallUtf8 t@(BS.S.I.SBS arr)) =
    T.B.writeN n $ \dest destOff -> do
        T.A.copyI dest destOff (T.A.Array arr) 0 n
  where n = BS.S.length t


fromString :: String -> SmallUtf8
fromString = fromText . T.pack

toString :: SmallUtf8 -> String
toString = T.unpack . toText

-- TODO: Verify that it's ASCII
fromAscii :: BS.ByteString -> SmallUtf8
fromAscii = SmallUtf8 . BS.S.toShort

toByteString :: SmallUtf8 -> BS.ByteString
toByteString (SmallUtf8 b) = BS.S.fromShort b

-- | Assumes 'BS.ByteString' contains correctly encoded UTF-8.
unsafeFromByteString :: BS.ByteString -> SmallUtf8
unsafeFromByteString = SmallUtf8 . BS.S.toShort

toByteStringBuilder :: SmallUtf8 -> BS.B.Builder
toByteStringBuilder (SmallUtf8 t) = BS.B.shortByteString t

toShortByteString :: SmallUtf8 -> BS.S.ShortByteString
toShortByteString (SmallUtf8 b) = b

unsafeFromShortByteString :: BS.S.ShortByteString -> SmallUtf8
unsafeFromShortByteString = SmallUtf8

instance Binary SmallUtf8 where
    get = do
        len <- getWord8
        len' <- case len of
          255 -> fromIntegral <$> getWord32le
          _   -> pure $ fromIntegral len
        SmallUtf8 . BS.S.toShort <$> getByteString len'
    {-# INLINE get #-}

    put (SmallUtf8 t)
      | BS.S.length t >= 0xffffffff
      = fail "Crazy long SmallUtf8"

      | BS.S.length t < 255
      = do putWord8 $ fromIntegral $ BS.S.length t
           putBuilder $ BS.B.shortByteString t

      | otherwise
      = do putWord8 255
           putWord32le $ fromIntegral $ BS.S.length t
           putBuilder $ BS.B.shortByteString t
    {-# INLINE put #-}

instance Aeson.ToJSON SmallUtf8 where
    toJSON = Aeson.String . toText
    toEncoding (SmallUtf8 x) = Aeson.unsafeToEncoding $
        BS.B.char7 '"' <> BS.B.shortByteString x <> BS.B.char7 '"'
    {-# INLINE toEncoding #-}

instance Aeson.ToJSONKey SmallUtf8 where
    toJSONKey = Aeson.ToJSONKeyText toText enc
      where enc (SmallUtf8 x) = Aeson.unsafeToEncoding $
              BS.B.char7 '"' <> BS.B.shortByteString x <> BS.B.char7 '"'
    {-# INLINEABLE toJSONKey #-}

instance Aeson.FromJSON SmallUtf8 where
    parseJSON (Aeson.String t) = pure $ fromText t
    parseJSON _                = fail "ToJson(SmallUtf8): Expected String"

instance Aeson.FromJSONKey SmallUtf8 where
    fromJSONKey = Aeson.FromJSONKeyText fromText

-- TODO: Serialise directly from SmallByteString
instance CBOR.Serialise SmallUtf8 where
    decode = do
        ty <- CBOR.peekTokenType
        case ty of
          CBOR.TypeBytes       -> unsafeFromByteString <$> CBOR.decodeBytes
          CBOR.TypeBytesIndef  -> unsafeFromByteString <$> CBOR.decodeBytes
          CBOR.TypeString      -> SmallUtf8 . CBOR.BA.toShortByteString <$> CBOR.decodeUtf8ByteArray
          CBOR.TypeStringIndef -> SmallUtf8 . CBOR.BA.toShortByteString <$> CBOR.decodeUtf8ByteArray
          other                -> fail $ "Serialise(SmallUtf8): Unknown token type "++show other
    encode (SmallUtf8 sbs) = CBOR.encodeUtf8ByteArray $ CBOR.BA.Sliced.fromShortByteString sbs
