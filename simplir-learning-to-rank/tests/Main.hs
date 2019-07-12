import Test.Tasty

import qualified SimplIR.LearningToRank.Tests

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ SimplIR.LearningToRank.Tests.tests
        ]
