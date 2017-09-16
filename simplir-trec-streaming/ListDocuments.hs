import Data.Monoid

import SimplIR.TrecStreaming
import SimplIR.DataSource
import Control.Monad (forM_)
import Pipes.Safe
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T
import Options.Applicative
import Codec.Compression.Lzma
import ReadKba

args :: Parser (Bool, [FilePath])
args =
    (,)
      <$> switch (short 'a' <> long "all" <> help "Dump document contents")
      <*> some (argument str (help "Thrift-encoded corpus fragment"))

main :: IO ()
main = do
    (dumpContents, fnames) <- execParser $ info (helper <*> args) mempty
    forM_ fnames $ \fname -> do
        putStrLn fname
        Just dsrc <- return $ parseDataSource localFile $ T.pack fname
        items <- readItems . BSL.toStrict <$> runSafeT (readKbaFile dsrc)
        let dump
              | dumpContents = show
              | otherwise    = T.unpack . getDocumentId . documentId
        putStrLn $ unlines $ map dump items
