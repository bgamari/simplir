{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module SimplIR.Types
    ( -- * Often-needed types
      DocumentName(..)
    , DocumentLength(..)
    , TermFrequency(..)
    , getTermFrequency
      -- * Positions within documents
    , Span(..)
    , Position(..)
      -- * Postings and related types
    , DocumentId(..)
    , Posting(..)
      -- ** Offset 'DocumentId's
    , DocIdDelta(..)
    , toDocIdDelta
    , docIdDelta
    , applyDocIdDelta
    ) where

import Data.String (IsString)
import Data.Binary
import Data.Monoid
import GHC.Generics
import Control.DeepSeq
import Data.Vector.Unboxed.Deriving
import qualified Data.Vector.Unboxed as VU
import qualified Data.Aeson as Aeson
import Data.Aeson ((.=))
import Test.QuickCheck
import qualified Data.SmallUtf8 as Utf8
import Data.SmallNat
import GHC.Stack (HasCallStack)

newtype DocumentId = DocId Int
                   deriving (Show, Eq, Ord, Enum, Binary)

instance Arbitrary DocumentId where
    arbitrary = DocId . getPositive <$> arbitrary

derivingUnbox "DocumentId"
  [t| DocumentId -> Int |]
  [| \(DocId n) -> n |]
  [| DocId |]

newtype DocumentName = DocName { getDocName :: Utf8.SmallUtf8 }
                     deriving (Show, Eq, Ord, Binary, IsString,
                               Aeson.ToJSON, Aeson.FromJSON)

instance Arbitrary DocumentName where
    arbitrary = do
        n <- arbitrary :: Gen Int
        return $ DocName $ Utf8.fromString $ "doc"++show n

-- | An interval within a discrete *.
data Span = Span { begin, end :: !Int }
          deriving (Eq, Ord, Show, Generic)

instance Binary Span
instance Aeson.ToJSON Span where
    toJSON (Span{..}) = Aeson.object [ "begin" .= begin, "end" .= end ]
    toEncoding (Span{..}) = Aeson.pairs ("begin" .= begin <> "end" .= end)
instance Aeson.FromJSON Span where
    parseJSON = Aeson.withObject "span" $ \o -> Span <$> o Aeson..: "begin" <*> o Aeson..: "end"

derivingUnbox "Span"
  [t| Span -> (Int, Int) |]
  [| \(Span a b) -> (a, b) |]
  [| \(a, b) -> Span a b |]

-- | A position within a tokenized document
data Position = Position { charOffset :: !Span
                         , tokenN     :: !Int
                         }
              deriving (Eq, Ord, Show, Generic)

instance Binary Position
instance NFData Position where
    rnf (Position {}) = ()
instance Aeson.ToJSON Position where
    toJSON (Position{..}) = Aeson.object [ "token_pos" .= tokenN, "char_pos" .= charOffset ]
    toEncoding (Position{..}) = Aeson.pairs ("token_pos" .= tokenN <> "char_pos" .= charOffset)
instance Aeson.FromJSON Position where
    parseJSON = Aeson.withObject "position" $ \o -> Position <$> o Aeson..: "token_pos" <*> o Aeson..: "char_pos"

derivingUnbox "Position"
  [t| Position -> (Span, Int) |]
  [| \(Position a b) -> (a, b) |]
  [| \(a, b) -> Position a b |]

-- | A posting is a document identifier paired with some content, representing
-- one or more occurrences of a term in that document. You typically see 'Posting'
-- used in a map over terms. For instance, term index for boolean retrieval
-- might have type @Map Term ('Posting' ())@ whereas a positional index might
-- have type @Map Term ('Posting' ['Position']@).
data Posting a = Posting { postingDocId :: !DocumentId
                         , postingBody :: !a
                         }
               deriving (Show, Functor, Generic)

instance Arbitrary p => Arbitrary (Posting p) where
    arbitrary = Posting <$> arbitrary <*> arbitrary

-- | Comparing first on the 'DocumentId'
deriving instance Ord a => Ord (Posting a)
deriving instance Eq a => Eq (Posting a)

instance Binary a => Binary (Posting a)
instance NFData a => NFData (Posting a) where
    rnf (Posting _ x) = rnf x

derivingUnbox "Posting"
  [t| forall a. VU.Unbox a => Posting a -> (DocumentId, a) |]
  [| \(Posting a b) -> (a, b) |]
  [| \(a, b) -> Posting a b |]

-- | The length of a document in tokens.
newtype DocumentLength = DocLength Int
                       deriving (Eq, Ord, Show, Enum, Binary, Aeson.ToJSON, Aeson.FromJSON)

-- | A number of occurrences of a 'Term'
newtype TermFrequency = TermFreq Int
                      deriving (Eq, Ord, Show, Enum, Binary)

getTermFrequency :: Real a => TermFrequency -> a
getTermFrequency (TermFreq x) = fromIntegral x

instance Monoid TermFrequency where
    mempty = TermFreq 0
    TermFreq a `mappend` TermFreq b = TermFreq (a + b)


-- | A difference between two 'DocumentId's
newtype DocIdDelta = DocIdDelta SmallNat
                   deriving (Show, Binary, Enum, Bounded, Ord, Eq)

-- | Lift an 'Int' into a 'DocIdDelta'
toDocIdDelta :: HasCallStack => Int -> DocIdDelta
toDocIdDelta n
  | n' >= minBound && n' < maxBound = DocIdDelta n'
  | otherwise                       = error "toDocIdDelta: Bad delta"
  where n' = fromIntegral n :: SmallNat
{-# INLINE toDocIdDelta #-}

instance Monoid DocIdDelta where
    mempty = DocIdDelta 0
    DocIdDelta a `mappend` DocIdDelta b = DocIdDelta (a + b)

-- | Take the difference between two 'DocumentId's
docIdDelta :: HasCallStack => DocumentId -> DocumentId -> DocIdDelta
docIdDelta (DocId a) (DocId b)
  | delta < 0  = error "negative DocIdDelta"
  | otherwise  = DocIdDelta $ fromIntegral delta
  where delta = b - a
{-# INLINE docIdDelta #-}

applyDocIdDelta :: DocumentId -> DocIdDelta -> DocumentId
applyDocIdDelta (DocId n) (DocIdDelta d) = DocId (n + fromIntegral d)
