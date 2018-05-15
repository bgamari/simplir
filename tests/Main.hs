import Test.Tasty

import qualified SimplIR.TopK
import qualified SimplIR.LearningToRank.Tests

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ SimplIR.TopK.tests
        , SimplIR.LearningToRank.Tests.tests
        ]
