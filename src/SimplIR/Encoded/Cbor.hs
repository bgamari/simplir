{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.Encoded.Cbor
    ( Encoded, decode, encode
    ) where

import Control.Exception
import qualified Codec.Serialise as S
import qualified Data.ByteString.Lazy as BSL

-- | A serialized value. This has the advantage of being typesafe yet allowing
-- zero-cost copying.
newtype Encoded a = Encoded BSL.ByteString
                  deriving (S.Serialise)

instance (Show a, S.Serialise a) => Show (Encoded a) where
    showsPrec _ = shows . decode

decode :: S.Serialise a => Encoded a -> a
decode (Encoded bs) = either uhOh id $ S.deserialiseOrFail bs
  where
    uhOh err = throw $ EncodedCborDeserialiseFailure bs err

data EncodedCborDeserialiseFailure
    = EncodedCborDeserialiseFailure BSL.ByteString S.DeserialiseFailure
    deriving (Show)
instance Exception EncodedCborDeserialiseFailure

encode :: S.Serialise a => a -> Encoded a
encode = Encoded . S.serialise
