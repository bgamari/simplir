{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.EncodedList
    ( EncodedList
    , toList
    , fromList
    ) where

import Data.Binary
import Data.Binary.Get (runGetOrFail)
import Data.Binary.Put (runPut)

import qualified Data.ByteString.Lazy as BS.L

import Data.List (unfoldr)

newtype EncodedList a = EncodedList BS.L.ByteString
                      deriving (Binary)

instance (Binary a, Show a) => Show (EncodedList a) where
    showsPrec _ xs = showString "fromList " . shows (toList xs)

uncons :: Binary a => EncodedList a -> Maybe (a, EncodedList a)
uncons (EncodedList bs) =
    case runGetOrFail getElement bs of
        Left  (_, _, err)        -> error $ "Error decoding EncodedList: "++err
        Right (rest, _, Nothing)
          | BS.L.null rest       -> Nothing
          | otherwise            -> error $ "Unexpected data reading EncodedList"
        Right (rest, _, Just x)  -> Just (x, EncodedList rest)
  where
    getElement = do
        tag <- getWord8
        case tag of
            0 -> return Nothing
            1 -> Just <$> get
            _ -> fail "EncodedList: unknown tag"

toList :: Binary a => EncodedList a -> [a]
toList = unfoldr uncons

fromList :: Binary a => [a] -> EncodedList a
fromList =
    EncodedList . runPut . foldr encodeCons encodeNil
  where
    encodeCons x rest = putWord8 1 >> put x >> rest
    encodeNil         = putWord8 0
