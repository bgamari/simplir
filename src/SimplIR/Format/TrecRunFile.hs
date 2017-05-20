{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A simple parser for the TREC run file format for retrieval result rankings.
module SimplIR.Format.TrecRunFile where

import Data.Semigroup
import Data.Maybe
import Control.Monad
import Control.Applicative

import qualified Data.Text as T
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy.Builder.Int as TB
import qualified Data.Text.Lazy.Builder.RealFloat as TB
import qualified Data.Text.Lazy.IO as TL
import Text.Trifecta

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

parseRunFile :: Parser [RankingEntry]
parseRunFile = catMaybes <$> many (fmap Just parseLine <|> whitespaceOnly)
  where whitespaceOnly = spaces >> newline >> pure Nothing

parseLine :: Parser RankingEntry
parseLine = do
    void $ many newline
    let textField = fmap T.pack $ some $ noneOf " \t\n"
    queryId <- textField
    void space
    void textField  -- reserved (Q1?)
    void space
    documentName <- textField
    void space
    documentRank <- fromIntegral <$> natural
    documentScore <- either realToFrac id <$> integerOrDouble
    methodName <- textField
    void (some newline) <|> eof
    return $ RankingEntry {..}

readRunFile :: FilePath -> IO [RankingEntry]
readRunFile fname =
    parseFromFile parseRunFile fname >>= maybe (fail "Failed to parse run file") pure

writeRunFile :: FilePath -> [RankingEntry] -> IO ()
writeRunFile fname entries =
    TL.writeFile fname $ TB.toLazyText $ mconcat
    [ TB.fromText (queryId e) <> " Q1 "
      <> TB.fromText (documentName e) <> " "
      <> TB.decimal (documentRank e) <> " "
      <> TB.realFloat (documentScore e) <> " "
      <> TB.fromText (methodName e) <> "\n"
    | e <- entries
    ]
