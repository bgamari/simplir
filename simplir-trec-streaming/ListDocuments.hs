import SimplIR.TrecStreaming
import Control.Monad (forM_)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T
import Options.Applicative
import Codec.Compression.Lzma

args :: Parser [FilePath]
args = some $ argument str (help "Thrift-encoded corpus fragment")

main :: IO ()
main = do
    fnames <- execParser $ info (helper <*> args) mempty
    forM_ fnames $ \fname -> do
        putStrLn fname
        items <- readItems . BSL.toStrict . decompress <$> BSL.readFile fname
        putStrLn $ unlines $ map (T.unpack . getDocumentId . documentId) items

