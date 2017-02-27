{-# LANGUAGE RecordWildCards #-}

-- | A simple parser for the TREC run file format for retrieval result rankings.
module SimplIR.Format.TrecRunFile where

import Data.Maybe
import Control.Monad
import Control.Applicative

import qualified Data.Text as T
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
    let textField = fmap T.pack $ some $ noneOf " "
    queryId <- textField
    void space
    documentName <- textField
    void space
    documentRank <- fromIntegral <$> natural
    void space
    documentScore <- double
    void space
    methodName <- textField
    void newline
    return $ RankingEntry {..}

readRunFile :: FilePath -> IO [RankingEntry]
readRunFile fname =
    parseFromFile parseRunFile fname >>= maybe (fail "Failed to parse run file") pure
