{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Types where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as BS.S
import qualified Data.Text as T
import Data.Vector.Unboxed.Deriving
import qualified Data.Vector.Unboxed as VU

newtype DocumentId = DocId Int
                   deriving (Show, Eq, Ord, Enum)

derivingUnbox "DocumentId"
  [t| DocumentId -> Int |]
  [| \(DocId n) -> n |]
  [| DocId |]

newtype DocumentName = DocName BS.S.ShortByteString
                     deriving (Show, Eq, Ord)

-- | An interval within a discrete *.
data Span = Span { begin, end :: !Int }
          deriving (Eq, Ord, Show)

derivingUnbox "Span"
  [t| Span -> (Int, Int) |]
  [| \(Span a b) -> (a, b) |]
  [| \(a, b) -> Span a b |]

-- | A position within a tokenized document
data Position = Position { charOffset :: !Span
                         , tokenN     :: !Int
                         }
              deriving (Eq, Ord, Show)

derivingUnbox "Position"
  [t| Position -> (Span, Int) |]
  [| \(Position a b) -> (a, b) |]
  [| \(a, b) -> Position a b |]

newtype Term = Term T.Text
             deriving (Eq, Ord, Show)


data Posting a = Posting !DocumentId !a
               deriving (Show, Functor)

derivingUnbox "Posting"
  [t| forall a. VU.Unbox a => Posting a -> (DocumentId, a) |]
  [| \(Posting a b) -> (a, b) |]
  [| \(a, b) -> Posting a b |]

type BoolPosting = Posting ()
type TermFreqPosting = Posting Int
type PositionalPosting = Posting (VU.Vector Position)
