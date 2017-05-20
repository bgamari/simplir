{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- | A simple parser for the common @qrel@ query relevance format.
module SimplIR.Format.QRel
    ( -- * Types
      QueryId
    , DocumentName
    , Entry(..)
      -- * Parsing
    , readQRel
      -- * Reexports
    , IsRelevant(..)
    ) where

import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.HashMap.Strict as HM
import qualified Data.HashMap.Lazy as HM.Lazy

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
        [queryId, _dump, documentName, rel] ->
          let relevance = case rel of "0" -> NotRelevant
                                      _   -> Relevant
          in Just $ Entry{..}

        _ -> Nothing

mapQRels :: [Entry ] -> HM.Lazy.HashMap QueryId [Entry]
mapQRels entries =
    HM.fromListWith (++) $
      [ (queryId entry, [entry])
      | entry <- entries
      ]
