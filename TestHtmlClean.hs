import HtmlClean

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy.IO as TLIO
import qualified Data.ByteString as BS
import qualified Data.Text.ICU.Convert as Conv
import Criterion.Main

main = do
    conv <- Conv.open "iso-8859-1" Nothing
    x <- Conv.toUnicode conv <$> BS.getContents
    --x <- TIO.getContents
    --defaultMain [ bench "clean" $ nf clean x ]
    TLIO.putStrLn $ docBody $ clean x
