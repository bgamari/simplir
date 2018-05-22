import Test.Tasty

import qualified SimplIR.TopK
import qualified SimplIR.Ranking.Evaluation.Tests

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ SimplIR.TopK.tests
        , SimplIR.Ranking.Evaluation.Tests.tests
        ]
