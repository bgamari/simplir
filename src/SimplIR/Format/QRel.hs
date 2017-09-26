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
      -- * Writing
    , writeQRel
      -- * Relevance
    , RelevanceScale(..)
    , IsRelevant(..)
    , GradedRelevance(..)
    ) where

import Data.List
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.Read as TR
import qualified Data.Text.IO as T
import qualified Data.Text.Lazy.IO as TL
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.HashMap.Strict as HM
import qualified Data.HashMap.Lazy as HM.Lazy

import SimplIR.LearningToRank

type QueryId = T.Text
type DocumentName = T.Text

data Entry rel = Entry { queryId      :: !QueryId
                       , documentName :: !DocumentName
                       , relevance    :: !rel
                       }

class RelevanceScale rel where
    parseRelevance :: T.Text -> rel
    formatRelevance :: rel -> T.Text

instance RelevanceScale IsRelevant where
    parseRelevance "0" = NotRelevant
    parseRelevance "1" = Relevant
    parseRelevance s   = error $ "binaryRelevance: unknown relevance: "++show s

    formatRelevance NotRelevant = "0"
    formatRelevance Relevant    = "1"

newtype GradedRelevance = GradedRelevance Int
                        deriving (Eq, Ord, Show)

instance RelevanceScale GradedRelevance where
    parseRelevance s =
        case TR.signed TR.decimal s of
            Right (n, _) -> GradedRelevance n
            Left e       -> error $ "gradedRelevance: invalid integer: "++show e++": "++show s
    formatRelevance = T.pack . show

readQRel :: forall rel. RelevanceScale rel => FilePath -> IO [Entry rel]
readQRel fname =
    mapMaybe parseLine . T.lines <$> T.readFile fname
  where
    parseLine :: T.Text -> Maybe (Entry rel)
    parseLine line =
      case T.words line of
        [queryId, _dump, documentName, rel] ->
          let relevance = parseRelevance rel
          in Just $ Entry{..}

        _ -> Nothing

mapQRels :: [Entry rel] -> HM.Lazy.HashMap QueryId [Entry rel]
mapQRels entries =
    HM.fromListWith (flip (++))
      [ (queryId entry, [entry])
      | entry <- entries
      ]

writeQRel :: RelevanceScale rel => FilePath -> [Entry rel] -> IO ()
writeQRel fname entries =
    TL.writeFile fname $ TB.toLazyText $ mconcat $ intersperse "\n"
    $ map toEntryLine entries
  where
    toEntryLine Entry{..} =
        mconcat $
        intersperse " "
        [ TB.fromText queryId
        , "0"
        , TB.fromText documentName
        , TB.fromText (formatRelevance relevance)
        ]
