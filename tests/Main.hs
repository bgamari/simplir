import Test.Tasty

import qualified DiskIndex.Tests
import qualified DiskIndex.Posting.Tests

main :: IO ()
main = do
    defaultMain $ testGroup "tests"
        [ DiskIndex.Tests.tests
        , DiskIndex.Posting.Tests.tests
        ]
