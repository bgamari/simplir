{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module SimplIR.TrecEval
  ( -- * Running @trec-eval@
    MissingQueryHandling(..)
  , runTrecEval
  , runTrecEval'
    -- * Parsing results
  , parseTrecEval
  , TrecEvalResult(..)
    -- * Metrics
  , Metric(..)
  , meanAvgPrec
  , rPrecision
  , recipRank
  ) where

import System.Process
import System.IO
import System.IO.Temp
import Data.Hashable

import SimplIR.Format.QRel as QRel
import SimplIR.Format.TrecRunFile as Run

newtype Metric = Metric { getMetricName :: String }
               deriving (Eq, Ord, Hashable, Show, Read)

meanAvgPrec :: Metric
meanAvgPrec = Metric "map"

rPrecision :: Metric
rPrecision = Metric "Rprec"

recipRank :: Metric
recipRank = Metric "recip_rank"

data TrecEvalResult = TrecEvalResult { trecEvalMetric :: Metric
                                     , trecEvalQuery  :: Maybe String
                                     , trecEvalScore  :: Double
                                     }
                    deriving (Show)

parseTrecEval :: String -> [TrecEvalResult]
parseTrecEval ents =
    [ TrecEvalResult { trecEvalMetric = Metric a
                     , trecEvalQuery = case b of "all" -> Nothing
                                                 _     -> Just b
                     , trecEvalScore = read c
                     }
    | l <- lines ents
    , [a,b,c] <- pure $ words l
    ]

data MissingQueryHandling = ExcludeMissingQueries
                          | IncludeMissingQueries -- ^ Assign a score of zero to queries with no results
                          deriving (Eq, Ord, Show, Bounded, Enum)

runTrecEval' :: MissingQueryHandling -> [Metric]
             -> FilePath -> FilePath -> IO [TrecEvalResult]
runTrecEval' missingQueryHandling metrics qrelPath runfilePath = do
    let args = missingQueryFlag ++ [qrelPath, runfilePath]
            ++ [ "-m" ++ metric | Metric metric <- metrics ]
    parseTrecEval <$> readProcess "trec_eval" args ""
  where
    missingQueryFlag = case missingQueryHandling of
                         ExcludeMissingQueries -> []
                         IncludeMissingQueries -> ["--complete_rel_info_wanted"]

runTrecEval :: MissingQueryHandling
            -> [Metric]
            -> [QRel.Entry Run.QueryId QRel.DocumentName QRel.IsRelevant]
            -> [Run.RankingEntry]
            -> IO [TrecEvalResult]
runTrecEval missingQueryHandling metrics qrels rankings =
    withSystemTempFile "temp.qrel" $ \qrelPath qrelHdl ->
      withSystemTempFile "temp.run" $ \runfilePath runfileHdl -> do
        hClose qrelHdl
        hClose runfileHdl
        writeRunFile runfilePath rankings
        writeQRel qrelPath qrels
        runTrecEval' missingQueryHandling metrics qrelPath runfilePath
