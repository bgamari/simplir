import Test.Tasty

import qualified SimplIR.TopK

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ SimplIR.TopK.tests
        ]
