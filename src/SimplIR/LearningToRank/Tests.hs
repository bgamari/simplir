{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module SimplIR.LearningToRank.Tests (tests) where

import qualified Data.Set as S
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Semigroup
import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.Tasty
import Test.Tasty.QuickCheck

import SimplIR.LearningToRank
import qualified SimplIR.TrecEval as TrecEval
import SimplIR.Format.TrecRunFile as Run
import SimplIR.Format.QRel as QRel
import SimplIR.LearningToRank as LearningToRank

data AssessedRanking a = AssessedRanking { relevant :: S.Set a
                                         , nonrelevant :: S.Set a
                                         , ranking :: [(Double, a)]
                                         }
                       deriving (Show)

instance (Ord a, Arbitrary a) => Arbitrary (AssessedRanking a) where
    arbitrary = do
        allDocs <- getNonEmpty <$> arbitrary
        relevant <- S.fromList <$> sublistOf allDocs
        let nonrelevant = S.fromList allDocs `S.difference` relevant
        rankingOrder <- suchThat
            (sublistOf (S.toList $ relevant <> nonrelevant) >>= shuffle)
            (not . null)
        let ranking = zip (map negate [1..]) rankingOrder
        return AssessedRanking {..}

newtype DocName = DocName T.Text
                deriving (Show, Ord, Eq)

instance Arbitrary DocName where
    arbitrary = toDocName . getPositive <$> arbitrary
      where
        toDocName :: Int -> DocName
        toDocName = DocName . T.pack . ("doc-"++) . show

runTrecEval :: TrecEval.Metric -> AssessedRanking DocName -> PropertyM IO Double
runTrecEval metric assessed = do
    let mkQRel rel (DocName doc) =
            QRel.Entry { queryId = "query"
                       , documentName = doc
                       , relevance = rel
                       }
        qrels = map (mkQRel Relevant) (S.toList $ relevant assessed)
             ++ map (mkQRel NotRelevant) (S.toList $ nonrelevant assessed)
        rankings = [ RankingEntry { queryId = "query"
                                  , documentName = doc
                                  , documentRank = rank
                                  , documentScore = score
                                  , methodName = "test"
                                  }
                   | (rank, (score, DocName doc)) <- zip [1..] (ranking assessed)
                   ]
    trecEvalResults <- run $ TrecEval.runTrecEval TrecEval.ExcludeMissingQueries [metric] qrels rankings
    return $ TrecEval.trecEvalScore $ head $ trecEvalResults

mapMatchesTrecEval :: AssessedRanking DocName -> Property
mapMatchesTrecEval assessed = monadicIO $ do
    score <- runTrecEval TrecEval.meanAvgPrec assessed
    monitor $ counterexample ("trec_eval="++show score++", us="++show score'')
    let relDiff = abs ((score'' - score) / score)
    return $ (score'' < 1e-5 && score == 0) || (relDiff < 1e-2)
  where
    getRelevance docName
      | docName `S.member` relevant assessed = Relevant
      | otherwise = NotRelevant

    -- trec_eval's treatment of NaNs is a bit questionable
    score''
      | isNaN score' = 0
      | otherwise    = score'

    score' = LearningToRank.meanAvgPrec (const $ S.size $ relevant assessed) Relevant
        $ M.singleton ("query" :: String) $ Ranking
        [ (score, (docName, rel))
        | (score, docName) <- ranking assessed
        , let rel = getRelevance docName
        ]

tests :: TestTree
tests = testGroup "LearningToRank"
    [ testProperty "MAP matches trec-eval" mapMatchesTrecEval
    ]
