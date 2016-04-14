{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Encoded
    ( Encoded, decode, encode
    ) where

import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BSL

-- | A serialized value. This has the advantage of being typesafe yet allowing
-- zero-cost copying.
newtype Encoded a = Encoded BSL.ByteString
                  deriving (B.Binary)

instance (Show a, B.Binary a) => Show (Encoded a) where
    showsPrec _ = shows . decode

decode :: B.Binary a => Encoded a -> a
decode (Encoded bs) = B.decode bs

encode :: B.Binary a => a -> Encoded a
encode = Encoded . B.encode
