import Control.Monad (msum)
import Control.Exception (evaluate)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Lazy.IO as TLIO
import qualified Data.ByteString as BS
import qualified Data.Text.ICU.Convert as Conv
import SimplIR.HTML.Clean

decodeIso8859 :: BS.ByteString -> IO T.Text
decodeIso8859 bs = do
    conv <- Conv.open "iso-8859-1" Nothing
    return $ Conv.toUnicode conv bs

main :: IO ()
main = do
    bs <- BS.getContents
    x <- msum $ map ($ bs)
        [ evaluate . TE.decodeUtf8
        , decodeIso8859
        , fail "Failed to decode"
        ]
    TLIO.putStrLn $ docBody $ clean x
