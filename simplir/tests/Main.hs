import Test.Tasty

import qualified SimplIR.TopK
import qualified SimplIR.Ranking.Evaluation.Tests
import qualified SimplIR.LearningToRank.Tests

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ SimplIR.TopK.tests
        , SimplIR.Ranking.Evaluation.Tests.tests
        , SimplIR.LearningToRank.Tests.tests
        ]
