{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A simple parser for the TREC run file format for retrieval result rankings.
module SimplIR.Format.TrecRunFile where

import Control.DeepSeq
import Data.Char
import Data.Maybe
import Data.Semigroup

import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.Int as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TB
import qualified Data.Text.Lazy.Read as TL.Read
import qualified Data.Text.Lazy.IO as TL

type QueryId = T.Text
type DocumentName = T.Text
type Rank = Int
type Score = Double
type MethodName = T.Text

data RankingEntry = RankingEntry { queryId       :: !QueryId
                                 , documentName  :: !DocumentName
                                 , documentRank  :: !Rank
                                 , documentScore :: !Score
                                 , methodName    :: !MethodName
                                 }
                  deriving (Show)

instance NFData RankingEntry where
    rnf r = r `seq` ()

readRunFile :: FilePath -> IO [RankingEntry]
readRunFile fname = do
    mapMaybe parse . TL.lines <$> TL.readFile fname
  where
    parse x
      | qid:_reserved:docName:rank:score:methodName:_ <- TL.words x
      = Just RankingEntry { queryId = TL.toStrict qid
                          , documentName = TL.toStrict docName
                          , documentRank = readError "rank" TL.Read.decimal rank
                          , documentScore = readError "score" TL.Read.double score
                          , methodName = TL.toStrict methodName
                          }
      | TL.all isSpace x = Nothing

    parse x = error $ "readRunFile: Unrecognized line in "++fname++": "++TL.unpack x

    readError :: String -> TL.Read.Reader a -> TL.Text -> a
    readError place reader str =
      case reader str of
        Left err -> error $ "readRunFile: "++fname++": Error parsing "++place++": "++err++": "++TL.unpack str
        Right (x,_) -> x


writeRunFile :: FilePath -> [RankingEntry] -> IO ()
writeRunFile fname entries =
    TL.writeFile fname $ TB.toLazyText $ mconcat
    [ TB.fromText (queryId e) <> " Q0 "
      <> TB.fromText (documentName e) <> " "
      <> TB.decimal (documentRank e) <> " "
      <> TB.realFloat (documentScore e) <> " "
      <> TB.fromText (methodName e) <> "\n"
    | e <- entries
    ]
