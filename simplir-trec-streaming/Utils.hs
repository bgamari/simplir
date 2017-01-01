module Utils where

import System.IO

import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Yaml as Yaml

import Types
import Query
import Parametric

type QueryFile = FilePath

readQueries :: QueryFile -> IO (M.Map QueryId QueryNode)
readQueries fname = do
    queries' <- either decodeError pure =<< Yaml.decodeFileEither fname
    let queries = getQueries queries'
    let allTerms = foldMap (S.fromList . collectFieldTerms FieldText) queries
    hPutStrLn stderr $ show (M.size queries)++" queries with "++show (S.size allTerms)++" unique terms"
    return queries
  where
    decodeError exc = fail $ "Failed to parse queries file "++fname++": "++show exc

type ParamsFile = FilePath

readParameters :: ParamsFile -> IO (M.Map ParamSettingName (Parameters Double))
readParameters fname = do
    either paramDecodeError (pure . getParamSets) =<< Yaml.decodeFileEither fname
  where
    paramDecodeError exc = fail $ "Failed to read parameters file "
