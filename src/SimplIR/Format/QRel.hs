{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A simple parser for the common @qrel@ query relevance format.
module SimplIR.Format.QRel
    ( -- * Types
      QueryId
    , DocumentName
    , Entry(..)
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

type QueryId = T.Text
type DocumentName = T.Text

data Entry = Entry { queryId      :: !QueryId
                   , documentName :: !DocumentName
                   , relevance    :: !IsRelevant
                   }

readQRel :: FilePath -> IO [Entry]
readQRel fname =
    mapMaybe parseLine . T.lines <$> T.readFile fname
  where
    parseLine :: T.Text -> Maybe Entry
    parseLine line =
      case T.words line of
        [queryId, _dump, docId, relevance] ->
          let rel = case relevance of "0" -> NotRelevant
                                      _   -> Relevant
          in Just (Entry queryId docId rel)

        _ -> Nothing
