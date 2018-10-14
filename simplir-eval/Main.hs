{-# LANGUAGE TypeApplications #-}

import Data.Function
import Data.List
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Options.Applicative

import qualified SimplIR.Statistics as Stats
import SimplIR.Format.QRel as QRel
import SimplIR.Format.TrecRunFile as Run
import SimplIR.Ranking as Ranking
import SimplIR.Ranking.Evaluation

data Metric = MAP

args :: Parser (FilePath, FilePath, [Metric])
args =
    (,,)
      <$> argument str (metavar "QREL" <> help "Query relevance (qrel) file")
      <*> argument str (metavar "RUN" <> help "Run results file")
      <*> some (option (str >>= metric) (short 'm' <> long "metric" <> help "metrics to compute"))
  where
    metric "map" = pure MAP
    metric m = fail $ "unknown metric '"++m++"'"

main :: IO ()
main = do
    (qrelPath, runPath, metrics) <- execParser $ info (helper <*> args) mempty
    -- Construct relevance judgments
    qrels <- QRel.readQRel @QRel.IsRelevant qrelPath
    let rels :: M.Map Run.QueryId (TotalRel, S.Set Run.DocumentName)
        rels =
            fmap (\relDocs -> (S.size relDocs, relDocs))
            $ M.fromListWith (<>)
            [ (QRel.queryId e, S.singleton $ QRel.documentName e)
            | e <- qrels ]

    -- Read (lazily) run file
    run <- Run.readRunFile runPath
    let queries :: [[Run.RankingEntry]]
        queries = groupBy ((==) `on` (\e -> (Run.queryId e, methodName e))) run

    -- Compute metrics
    let metrics :: M.Map Run.MethodName (Stats.Mean Double)
        metrics = M.fromListWith (<>)
            [ (method, score)
            | queryEntries@(e0:_) <- queries
            , let qid = Run.queryId e0
                  method = Run.methodName e0
                  Just (totalRel, relDocs) = M.lookup qid rels

            , let ranking :: Ranking Double (Run.DocumentName, Bool)
                  ranking = Ranking.fromSortedList
                      [ (Run.documentScore e, (Run.documentName e, rel))
                      | e <- queryEntries
                      , let rel = Run.documentName e `S.member` relDocs
                      ]

                  score :: Stats.Mean Double
                  Just score = Stats.one <$> avgPrec True totalRel ranking
            ]
    print $ fmap Stats.getMean metrics
    return ()

