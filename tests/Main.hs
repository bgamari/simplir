import Test.Tasty

import qualified DiskIndex.Tests
import qualified DiskIndex.Posting.Tests
import qualified TopK

main :: IO ()
main =
    defaultMain $ testGroup "tests"
        [ DiskIndex.Tests.tests
        , DiskIndex.Posting.Tests.tests
        , TopK.tests
        ]
