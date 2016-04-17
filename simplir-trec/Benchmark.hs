import Pipes
import qualified Pipes.Prelude as P.P
import qualified Pipes.Text.IO as P.T.IO
import System.IO
import TREC
import qualified Data.Text.Lazy.IO as T.L
import Criterion.Main

fname = "data/robust04/docs/ft91.dat"

main :: IO ()
main = do
    defaultMain [ bench "Pipes" $ whnfIO $ do
                      hdl <- openFile fname ReadMode
                      P.P.length $ TREC.trecDocuments' (P.T.IO.fromHandle hdl)
                , bench "list" $ whnfIO $ do
                      length . TREC.trecDocuments <$> T.L.readFile fname
                ]
    -- h <- P.P.toListM $ TREC.trecDocuments' (P.T.IO.fromHandle hdl)
    --print $ length h
