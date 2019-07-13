{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}

import qualified Data.Heap as H
import Data.Ord (Down(..))
import Data.Coerce
import Data.Profunctor
import Data.Foldable
import Data.Monoid
import qualified Data.Map as M
import Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as BS
import Data.Monoid
import Options.Applicative
import Codec.Compression.GZip
import Pipes
import qualified Pipes.Prelude as P.P
import qualified Control.Foldl as Foldl

import Control.Foldl.Map
import SimplIR.TopK
import SimplIR.Pipes.Utils
import Query (QueryId)
import Types
import Parametric

main :: IO ()
main = do
    let args =
            (,,,)
              <$> option str (help "output filename" <> short 'o' <> long "output")
              <*> optional (option auto (help "how many results?" <> short 'N' <> long "length"))
              <*> optional (option auto (help "top results per input ranking" <> short 'K' <> long "topk"))
              <*> some (argument str (metavar "RANKING" <> help "ranking file"))

    (outputFile, n, k, fnames) <- execParser $ info (helper <*> args) mempty


    let sortAndCut :: Foldl.Fold ScoredDocument [ScoredDocument]
        sortAndCut =
            case n of
              Just n' -> topK n'
              Nothing -> dimap (H.singleton . Down) (coerce . toList) Foldl.mconcat -- just sort

    let folder = Foldl.generalize $ multiFold sortAndCut

    let cutInputRankings :: [ScoredDocument] -> [ScoredDocument]
        cutInputRankings =
          case k of
            Just k' -> take k'
            Nothing -> id

    let producer :: Producer ((QueryId, ParamSettingName), ScoredDocument) IO ()
        producer =
              each fnames
          >-> P.P.chain putStrLn
          >-> P.P.mapM readRanking
          >-> P.P.map (\(Results results) -> results)
          >-> cat'                        @(M.Map (QueryId, ParamSettingName) [ScoredDocument])
          >-> P.P.map (M.toList . fmap cutInputRankings)
          >-> P.P.concat
          >-> cat'                        @((QueryId, ParamSettingName), [ScoredDocument])
          >-> P.P.mapFoldable (\(x,ys) -> [ (x,y)
                                          | y <- ys
                                          , let s = realToFrac $ scoredRankScore y :: Double
                                          , not $ isNaN s || isInfinite s
                                          ])

    r <- foldProducer folder producer  --- producer >> folder >> output
    BS.writeFile outputFile $ encode $ Results r

readRanking :: FilePath -> IO Results
readRanking fname =
    BS.readFile fname >>= either fail return . Aeson.eitherDecode . decompress
