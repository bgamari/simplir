import Test.Tasty

import qualified SimplIR.DiskIndex.Tests
import qualified SimplIR.DiskIndex.Posting.Tests
import qualified SimplIR.DiskIndex.Posting.Collect
import qualified SimplIR.DiskIndex.Posting2.Tests
import qualified SimplIR.DiskIndex.Posting2.Collect

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ SimplIR.DiskIndex.Tests.tests
        , SimplIR.DiskIndex.Posting.Tests.tests
        , SimplIR.DiskIndex.Posting.Collect.tests
        , SimplIR.DiskIndex.Posting2.Tests.tests
        , SimplIR.DiskIndex.Posting2.Collect.tests
        ]
