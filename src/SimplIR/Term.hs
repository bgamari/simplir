{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.Term where

import Data.String (IsString)
import Control.DeepSeq
import Data.Binary
import Data.Hashable
import Test.QuickCheck
import qualified Data.Text as T
import qualified Data.SmallUtf8 as Utf8
import Data.Aeson (FromJSON, ToJSON)

newtype Term = Term Utf8.SmallUtf8
             deriving (Eq, Ord, NFData, Hashable, IsString, Binary, FromJSON, ToJSON)

instance Show Term where
    showsPrec _ (Term s) = showString "fromString " . shows (Utf8.toString s)

fromText :: T.Text -> Term
fromText = Term . Utf8.fromText

toText :: Term -> T.Text
toText (Term t) = Utf8.toText t

toString :: Term -> String
toString (Term t) = Utf8.toString t

fromString :: String -> Term
fromString = fromText . T.pack

instance Arbitrary Term where
    arbitrary = fromString <$> vectorOf 3 (choose ('a','e'))
