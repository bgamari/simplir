{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | A simple parser for the common @qrel@ query relevance format.
module SimplIR.Format.QRel
    ( -- * Types
      QueryId
    , DocumentName
    , Entry(..)
      -- * Parsing
    , readQRel
    , mapQRels
      -- * Relevance
    , RelevanceScale
    , binaryRelevance
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

data Entry rel = Entry { queryId      :: !QueryId
                       , documentName :: !DocumentName
                       , relevance    :: !rel
                       }

type RelevanceScale rel = T.Text -> rel

binaryRelevance :: RelevanceScale IsRelevant
binaryRelevance "0" = NotRelevant
binaryRelevance "1" = Relevant
binaryRelevance s   = error $ "binaryRelevance: unknown relevance: "++show s

readQRel :: forall rel. RelevanceScale rel -> FilePath -> IO [Entry rel]
readQRel parseRel fname =
    mapMaybe parseLine . T.lines <$> T.readFile fname
  where
    parseLine :: T.Text -> Maybe (Entry rel)
    parseLine line =
      case T.words line of
        [queryId, _dump, documentName, rel] ->
          let relevance = parseRel rel
          in Just $ Entry{..}

        _ -> Nothing

mapQRels :: [Entry rel] -> HM.Lazy.HashMap QueryId [Entry rel]
mapQRels entries =
    HM.fromListWith (flip (++))
      [ (queryId entry, [entry])
      | entry <- entries
      ]
