{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Term where

import Data.Char (ord)
import Data.String
import Control.DeepSeq
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Hashable
import qualified Data.ByteString.Short as BS.S
import qualified Data.ByteString.Builder as BS.B
import qualified Data.Text as T
import qualified Data.Text.Encoding as T.E
import Test.QuickCheck

newtype Term = Term BS.S.ShortByteString
             deriving (Eq, Ord, Show, NFData, Hashable, IsString)

fromText :: T.Text -> Term
fromText = Term . BS.S.toShort . T.E.encodeUtf8

toText :: Term -> T.Text
toText (Term t) = T.E.decodeUtf8 $ BS.S.fromShort t

fromString :: String -> Term
fromString = fromText . T.pack

instance Binary Term where
    get = {-# SCC getTerm #-} do
        len <- fromIntegral <$> getWord8
        Term . BS.S.toShort <$> getByteString len
    put (Term t) = do
        putWord8 $ fromIntegral $ BS.S.length t
        putBuilder $ BS.B.shortByteString t

instance Arbitrary Term where
    arbitrary = Term . BS.S.pack . map (fromIntegral . ord) <$> vectorOf 3 (choose ('a','e'))
