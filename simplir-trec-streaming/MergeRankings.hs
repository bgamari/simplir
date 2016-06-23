import qualified Data.Map as M
import Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as BS
import Options.Applicative
import Pipes
import qualified Pipes.Prelude as P.P
import qualified Control.Foldl as Foldl

import Control.Foldl.Map
import SimplIR.TopK
import SimplIR.Utils
import Types

main :: IO ()
main = do
    let args = some $ argument str (help "ranking file")
    fnames <- execParser $ info (helper <*> args) mempty

    let readRanking :: FilePath -> IO [Ranking]
        readRanking fname = either fail id . Aeson.eitherDecode <$> BS.readFile fname

    r <- foldProducer (Foldl.generalize $ multiFold $ topK 100)
        $  each fnames
       >-> P.P.mapM readRanking
       >-> P.P.concat
       >-> P.P.mapFoldable (\r -> [(rankingQueryId r, res) | res <- rankingResults r])

    BS.writeFile "merged.json" $ encode $ map (uncurry Ranking) (M.assocs r)
