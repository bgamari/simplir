import Criterion.Main
import qualified Data.Text.IO as TIO
import Text.HTML.Clean

main :: IO ()
main = do
    x <- TIO.getContents
    defaultMain [ bench "clean" $ nf clean x ]
