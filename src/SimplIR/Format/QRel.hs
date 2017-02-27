{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A simple parser for the common @qrel@ query relevance format.
module SimplIR.Format.QRel
    ( -- * Types
      QueryId(..)
    , DocumentName(..)
    , QRel
      -- * Parsing
    , readQRel
    ) where

import Data.Maybe
import Data.String

import Data.Hashable
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.IO as T

import SimplIR.LearningToRank

newtype QueryId = QueryId T.Text
                deriving (Show, IsString, Eq, Ord, Hashable)

newtype DocumentName = DocumentName T.Text
                     deriving (Show, IsString, Eq, Ord, Hashable)

type QRel = [(QueryId, DocumentName, IsRelevant)]

readQRel :: FilePath -> IO QRel
readQRel fname =
    mapMaybe parseLine . T.lines <$> T.readFile fname
  where
    parseLine :: T.Text -> Maybe (QueryId, DocumentName, IsRelevant)
    parseLine line =
      case T.words line of
        [queryId, _dump, docId, relevance] ->
          let rel = case relevance of "0" -> NotRelevant
                                      _   -> Relevant
          in Just (QueryId queryId, DocumentName docId, rel)

        _ -> Nothing
