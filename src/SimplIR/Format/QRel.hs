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
    , RelevanceScale
    , gradedRelevance
    , binaryRelevance
    , IsRelevant(..)
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

data RelevanceScale rel = RelevanceScale { parseRel :: T.Text -> rel
                                         , formatRel :: rel -> T.Text
                                         }

binaryRelevance :: RelevanceScale IsRelevant
binaryRelevance = RelevanceScale {..}
  where
    parseRel "0" = NotRelevant
    parseRel "1" = Relevant
    parseRel s   = error $ "binaryRelevance: unknown relevance: "++show s

    formatRel NotRelevant = "0"
    formatRel Relevant    = "1"

gradedRelevance :: RelevanceScale Int
gradedRelevance = RelevanceScale {..}
  where
    parseRel s =
        case TR.signed TR.decimal s of
            Right (n, _) -> n
            Left e       -> error $ "gradedRelevance: invalid integer: "++show e++": "++show s
    formatRel = T.pack . show

readQRel :: forall rel. RelevanceScale rel -> FilePath -> IO [Entry rel]
readQRel rscale fname =
    mapMaybe parseLine . T.lines <$> T.readFile fname
  where
    parseLine :: T.Text -> Maybe (Entry rel)
    parseLine line =
      case T.words line of
        [queryId, _dump, documentName, rel] ->
          let relevance = parseRel rscale rel
          in Just $ Entry{..}

        _ -> Nothing

mapQRels :: [Entry rel] -> HM.Lazy.HashMap QueryId [Entry rel]
mapQRels entries =
    HM.fromListWith (flip (++))
      [ (queryId entry, [entry])
      | entry <- entries
      ]

writeQRel :: RelevanceScale rel -> FilePath -> [Entry rel] -> IO ()
writeQRel rscale fname entries =
    TL.writeFile fname $ TB.toLazyText $ mconcat $ intersperse "\n"
    $ map toEntryLine entries
  where
    toEntryLine Entry{..} =
        mconcat $
        intersperse " "
        [ TB.fromText queryId
        , "0"
        , TB.fromText documentName
        , TB.fromText (formatRel rscale relevance)
        ]
