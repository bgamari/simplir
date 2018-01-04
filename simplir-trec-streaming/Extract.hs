{-# LANGUAGE RecordWildCards #-}

import Data.Monoid
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BS.L
import Data.Aeson as Aeson
import Control.Exception (bracket)
import Options.Applicative
import Pipes
import Pipes.Safe hiding (bracket)
import qualified Pinch
import qualified Pinch.Protocol as Pinch
import qualified Pinch.Internal.Builder as Pinch
import qualified Pipes.Prelude as P.P
import qualified Control.Foldl as Foldl
import System.FilePath

import Control.Concurrent (setNumCapabilities)
import Control.Concurrent.Map

import Control.Foldl.Map
import qualified Data.SmallUtf8 as Utf8
import SimplIR.TrecStreaming as Kba
import SimplIR.Utils
import SimplIR.DataSource
import SimplIR.Types
import Types
import ReadKba

main :: IO ()
main = do
    let args =
            (,,)
              <$> option auto (help "how many results?" <> short 'N' <> long "count")
              <*> option auto (help "how many threads?" <> short 'j' <> long "jobs" <> value 1)
              <*> some (argument str (metavar "RANKING" <> help "ranking file"))
    (k, threads, fnames) <- execParser $ info (helper <*> args) mempty

    let getDocs :: ScoredDocument -> M.Map (DataSource (SafeT IO)) (S.Set DocumentName)
        getDocs (ScoredDocument{scoredDocumentInfo=DocInfo{..}})
          | Just dsrc <- parseDataSource localFile docArchive =
                M.singleton dsrc (S.singleton docName)
          | otherwise = mempty

    neededDocs <- foldProducer (Foldl.generalize mconcatMaps)
       $  each fnames
      >-> P.P.mapM readRanking
      >-> P.P.mapFoldable (\(Results ranking) -> map getDocs $ foldMap (take k) $ M.elems ranking)

    let nDocs = getSum $ foldMap (Sum . S.size) neededDocs
    putStrLn $ "Extracting "++show nDocs++" documents in "++show (M.size neededDocs)++" archives"
    setNumCapabilities threads
    void $ mapConcurrentlyL threads (uncurry dumpDocuments) $ M.assocs neededDocs

dumpDocuments :: DataSource (SafeT IO) -> S.Set DocumentName -> IO ()
dumpDocuments dsrc docs = do
    putStrLn $ "Dumping "++show dsrc
    takenDocs <- takeDocuments docs . BS.L.toStrict <$> runSafeT (readKbaFile dsrc)
    let outPath = dataSourceFileName dsrc <.> "sc.extracted"
    BS.writeFile outPath
        $ Pinch.runBuilder
        $ foldMap (Pinch.serializeValue Pinch.binaryProtocol) takenDocs

takeDocuments :: S.Set DocumentName -> BS.ByteString -> [Pinch.Value Pinch.TStruct]
takeDocuments docs = go
  where
    go bs
      | BS.null bs = []
      | docName `S.member` docs = val : go bs'
      | otherwise = go bs'
      where
        (bs', val) = either error id $ Pinch.deserializeValue' Pinch.binaryProtocol bs
        item = either error id $ Pinch.runParser (parseStreamItem val)
        docName = DocName (Utf8.fromText $ Kba.getDocumentId $ Kba.documentId item)

readRanking :: FilePath -> IO Results
readRanking fname =
    BS.L.readFile fname >>= either fail return . Aeson.eitherDecode
