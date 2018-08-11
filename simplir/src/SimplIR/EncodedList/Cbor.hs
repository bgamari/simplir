{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.EncodedList.Cbor
    ( EncodedList
    , uncons
    , toList
    , fromList
    , map
    , CborListDeserialiseError(..)
    ) where

import Control.Exception
import Data.Semigroup
import Data.List (unfoldr)
import Prelude hiding (map)

import Codec.Serialise
import qualified Codec.CBOR.Read as Read
import qualified Codec.CBOR.Write as Write
import qualified Data.ByteString.Lazy as BS.L

newtype EncodedList a = EncodedList { getEncodedList :: BS.L.ByteString }
                      deriving (Serialise)

instance Semigroup (EncodedList a) where
    EncodedList a <> EncodedList b = EncodedList (a <> b)
    sconcat = EncodedList . foldMap getEncodedList

instance Monoid (EncodedList a) where
    mempty = EncodedList mempty
    mappend = (<>)
    mconcat = EncodedList . foldMap getEncodedList

instance (Serialise a, Show a) => Show (EncodedList a) where
    showsPrec _ xs = showString "fromList " . shows (toList xs)

data CborListDeserialiseError = CborListDeserialiseError Read.DeserialiseFailure
                              deriving (Show)
instance Exception CborListDeserialiseError

uncons :: Serialise a => EncodedList a -> Maybe (a, EncodedList a)
uncons (EncodedList bs)
  | BS.L.null bs = Nothing
  | otherwise    = case Read.deserialiseFromBytes decode bs of
                     Right (rest, x) -> Just (x, EncodedList rest)
                     Left err        -> throw $ CborListDeserialiseError err

toList :: Serialise a => EncodedList a -> [a]
toList = unfoldr uncons

fromList :: Serialise a => [a] -> EncodedList a
fromList =
    EncodedList . Write.toLazyByteString . foldMap encode

map :: (Serialise a, Serialise b)
    => (a -> b) -> EncodedList a -> EncodedList b
map f = fromList . fmap f . toList
