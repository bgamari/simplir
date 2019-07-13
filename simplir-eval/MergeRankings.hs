import Data.Function
import Data.Profunctor
import Control.Foldl.HashMap
import qualified Control.Foldl as Foldl
import Data.Monoid
import qualified Data.HashMap.Strict as HM
import SimplIR.TopK as TopK
import SimplIR.Format.TrecRunFile
import Options.Applicative

parser :: Parser (FilePath, Int, [FilePath])
parser =
    (,,)
      <$> option str (short 'o' <> long "output" <> metavar "RUNFILE" <> help "Output run file")
      <*> option auto (short 'k' <> long "results" <> metavar "N" <> help "Desired ranking size per query")
      <*> some (argument str (metavar "RUNFILE" <> help "Run files to be merged"))

newtype ComparingScore = ComparingScore { getComparingScore :: RankingEntry }

instance Eq ComparingScore where
    (==) = (==) `on` documentScore . getComparingScore

instance Ord ComparingScore where
    compare = compare `on` documentScore . getComparingScore

main :: IO ()
main = do
    (output, k, inputs) <- execParser $ info parser mempty
    files <- mapM readRunFile inputs
    let withQuery :: RankingEntry -> (QueryId, ComparingScore)
        withQuery ent = (queryId ent, ComparingScore ent)
        isZeroScore ent = documentScore ent == 0
        results :: HM.HashMap QueryId [ComparingScore]
        results = Foldl.fold (lmap withQuery $ multiFold $ TopK.topK k)
                  $ filter (not . isZeroScore) $ concat files
    writeRunFile output
        [ ent { documentRank = rank }
        | queryRanking <- HM.elems results
        , (rank, ComparingScore ent) <- zip [1..] queryRanking
        ]
