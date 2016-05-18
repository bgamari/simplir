import Test.Tasty

import qualified DiskIndex.Tests

main :: IO ()
main = do
    defaultMain $ testGroup "tests"
        [ DiskIndex.Tests.tests ]
