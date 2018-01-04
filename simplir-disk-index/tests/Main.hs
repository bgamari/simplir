import Test.Tasty

import qualified SimplIR.DiskIndex.Tests
import qualified SimplIR.DiskIndex.Posting.Tests
import qualified SimplIR.DiskIndex.Posting.Collect

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ SimplIR.DiskIndex.Tests.tests
        , SimplIR.DiskIndex.Posting.Tests.tests
        , SimplIR.DiskIndex.Posting.Collect.tests
        ]
