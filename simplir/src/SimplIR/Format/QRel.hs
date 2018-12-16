{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

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
import Data.Hashable
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

data Entry query doc rel = Entry { queryId      :: !query
                                 , documentName :: !doc
                                 , relevance    :: !rel
                                 }

class RelevanceScale rel where
    parseRelevance :: T.Text -> rel
    formatRelevance :: rel -> T.Text

instance RelevanceScale T.Text where
    parseRelevance = id
    formatRelevance = id

instance RelevanceScale IsRelevant where
    parseRelevance "0" = NotRelevant
    parseRelevance "1" = Relevant
    parseRelevance s   = error $ "binaryRelevance: unknown relevance: "++show s

    formatRelevance NotRelevant = "0"
    formatRelevance Relevant    = "1"

newtype GradedRelevance = GradedRelevance {unGradedRelevance:: Int}
                        deriving (Eq, Ord, Show, Hashable)

instance RelevanceScale GradedRelevance where
    parseRelevance s =
        case TR.signed TR.decimal s of
            Right (n, _) -> GradedRelevance n
            Left e       -> error $ "gradedRelevance: invalid integer: "++show e++": "++show s
    formatRelevance (GradedRelevance rel) = T.pack $ show rel

readQRel :: forall rel. RelevanceScale rel => FilePath -> IO [Entry QueryId DocumentName rel]
readQRel fname =
    mapMaybe parseLine . T.lines <$> T.readFile fname
  where
    parseLine :: T.Text -> Maybe (Entry QueryId DocumentName rel)
    parseLine line =
      case T.words line of
        [queryId, _dump, documentName, rel] ->
          let relevance = parseRelevance rel
          in Just $ Entry{..}

        _ -> Nothing

mapQRels :: (Ord query, Hashable query)
         => [Entry query doc rel] -> HM.Lazy.HashMap query [Entry query doc rel]
mapQRels entries =
    HM.fromListWith (flip (++))
      [ (queryId entry, [entry])
      | entry <- entries
      ]

writeQRel :: RelevanceScale rel => FilePath -> [Entry QueryId  DocumentName rel] -> IO ()
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
