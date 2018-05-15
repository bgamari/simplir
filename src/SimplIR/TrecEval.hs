{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module SimplIR.TrecEval
  ( -- * Running @trec-eval@
    runTrecEval
  , runTrecEval'
    -- * Parsing results
  , parseTrecEval
    -- * Metrics
  , TrecEvalMetric(..)
  , meanAvgPrec
  , rPrecision
  , recipRank
  ) where

import System.Process
import System.IO.Temp
import Data.Hashable

import SimplIR.Format.QRel as QRel
import SimplIR.Format.TrecRunFile as Run

newtype TrecEvalMetric = TrecEvalMetric String
                       deriving (Eq, Ord, Hashable, Show, Read)

meanAvgPrec :: TrecEvalMetric
meanAvgPrec = TrecEvalMetric "map"

rPrecision :: TrecEvalMetric
rPrecision = TrecEvalMetric "Rprec"

recipRank :: TrecEvalMetric
recipRank = TrecEvalMetric "recip_rank"

data TrecEvalResult = TrecEvalResult { trecEvalMetric :: TrecEvalMetric
                                     , trecEvalQuery  :: Maybe String
                                     , trecEvalScore  :: Double
                                     }

parseTrecEval :: String -> [TrecEvalResult]
parseTrecEval ents =
    [ TrecEvalResult { trecEvalMetric = TrecEvalMetric a
                     , trecEvalQuery = case b of "all" -> Nothing
                                                 _     -> Just b
                     , trecEvalScore = read c
                     }
    | l <- lines ents
    , [a,b,c] <- pure $ words l
    ]

data MissingQueryHandling = ExcludeMissingQueries
                          | IncludeMissingQueries -- ^ Assign a score of zero to queries with no results

runTrecEval' :: MissingQueryHandling -> FilePath -> FilePath -> IO [TrecEvalResult]
runTrecEval' missingQueryHandling qrelPath runfilePath = do
    let args = missingQueryFlag ++ [qrelPath, runfilePath]
    parseTrecEval <$> readProcess "trec-eval" args ""
  where
    missingQueryFlag = case missingQueryHandling of
                         ExcludeMissingQueries -> []
                         IncludeMissingQueries -> ["--complete_rel_info_wanted"]

runTrecEval :: MissingQueryHandling
            -> [QRel.Entry Run.QueryId QRel.DocumentName QRel.IsRelevant]
            -> [Run.RankingEntry]
            -> IO [TrecEvalResult]
runTrecEval missingQueryHandling qrels rankings =
    withSystemTempFile "temp.qrel" $ \qrelPath _ ->
      withSystemTempFile "temp.run" $ \runfilePath _ -> do
        writeRunFile runfilePath rankings
        writeQRel qrelPath qrels
        runTrecEval' missingQueryHandling qrelPath runfilePath
