{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Main where

import Control.Monad (when)
import GHC.Generics
import Data.Semigroup hiding (option)

import Data.Aeson as Aeson
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Vector.Unboxed as VU
import Options.Applicative
import System.Random

import SimplIR.LearningToRank
import qualified SimplIR.Format.TrecRunFile as Run
import qualified SimplIR.Format.QRel as QRel

main :: IO ()
main = do
    mode <- execParser $ info modes fullDesc
    mode

modes :: Parser (IO ())
modes = subparser $
        command "learn" (info predictMode mempty)
     <> command "predict" (info predictMode mempty)

type FeatureFiles = [(FeatureName, FilePath)]

optFeatureFiles :: Parser FeatureFiles
optFeatureFiles =
    some $ argument featureFile $ metavar "FEATURE=PATH" <> help "Feature name and run file path"
  where
    featureFile :: ReadM (FeatureName, FilePath)
    featureFile = str >>= parse
      where
        parse s
          | (name,path) <- break (/= '=') s
          , not $ null name
          , not $ null path
          = return (FeatureName $ T.pack name, tail path)
          | otherwise = fail "Mal-formed feature file: expected '[feature]=[path]'"

newtype FeatureName = FeatureName T.Text
                    deriving (Ord, Eq, Show, ToJSON, FromJSON, ToJSONKey, FromJSONKey)

data Model = Model { modelWeights :: M.Map FeatureName Double
                   }
           deriving (Show, Generic)
instance ToJSON Model
instance FromJSON Model

toDocFeatures :: S.Set FeatureName
              -> M.Map FeatureName [Run.RankingEntry]
              -> M.Map (Run.QueryId, QRel.DocumentName) Features
toDocFeatures allFeatures runFiles =
    fmap (toFeatures allFeatures) docFeatures
  where
    docFeatures :: M.Map (Run.QueryId, QRel.DocumentName) (M.Map FeatureName Double)
    docFeatures = M.fromListWith M.union
                [ ( (Run.queryId entry, Run.documentName entry)
                  , M.singleton featureName (Run.documentScore entry) )
                | (featureName, ranking) <- M.toList runFiles
                , entry <- ranking
                ]

learnMode :: Parser (IO ())
learnMode =
    run <$> option str (short 'o' <> long "output" <> metavar "OUTPUT" <> help "Output model file")
        <*> option str (short 'q' <> long "qrel" <> metavar "QREL" <> help "qrel file")
        <*> optFeatureFiles
  where
    run :: FilePath -> FilePath -> [(FeatureName, FilePath)] -> IO ()
    run modelFile qrelFile featureFiles = do
        runFiles <- traverse Run.readRunFile $ M.fromListWith (error "Duplicate feature") featureFiles
        qrel <- QRel.readQRel qrelFile
        gen0 <- newStdGen
        let relevance :: M.Map (Run.QueryId, QRel.DocumentName) IsRelevant
            relevance = M.fromList [ ((qid, doc), rel) | QRel.Entry qid doc rel <- qrel ]

            docFeatures = toDocFeatures (M.keysSet runFiles) runFiles

            franking :: M.Map Run.QueryId [(QRel.DocumentName, Features, IsRelevant)]
            franking = M.fromListWith (++)
                       [ (qid, [(doc, features, rel)])
                       | ((qid, doc), features) <- M.assocs docFeatures
                       , let rel = M.findWithDefault NotRelevant (qid, doc) relevance
                       ]
            totalRel = M.fromListWith (+) [ (qid, n)
                                          | QRel.Entry qid _ rel <- qrel
                                          , let n = case rel of Relevant -> 1
                                                                NotRelevant -> 0 ]
            metric :: ScoringMetric IsRelevant Run.QueryId QRel.DocumentName
            metric = meanAvgPrec (totalRel M.!) Relevant
            weights0 :: Features
            weights0 = Features $ VU.replicate (M.size runFiles) 1
            iters = coordAscent gen0 metric weights0 franking
            hasConverged (a,_) (b,_) = abs (a-b) < 1e-7
            (evalScore, Features weights) = last $ untilConverged hasConverged iters
            modelWeights = M.fromList $ zip (M.keys runFiles) (VU.toList weights)

        print evalScore
        BSL.writeFile modelFile $ Aeson.encode $ Model {..}

toFeatures :: S.Set FeatureName -> M.Map FeatureName Double -> Features
toFeatures allFeatures features =
    Features $ VU.fromList [ features M.! f | f <- S.toList allFeatures ]

toWeights :: Model -> Features
toWeights (Model weights) =
    Features $ VU.fromList $ M.elems weights

untilConverged :: (a -> a -> Bool) ->  [a] -> [a]
untilConverged eq xs = map snd $ takeWhile (\(a,b) -> not $ a `eq` b) $ zip xs (tail xs)

predictMode :: Parser (IO ())
predictMode =
    run <$> option str (short 'm' <> long "model" <> metavar "MODEL" <> help "Input model file")
        <*> optFeatureFiles
  where
    run :: FilePath -> [(FeatureName, FilePath)] -> IO ()
    run modelFile featureFiles = do
        runFiles <- traverse Run.readRunFile $ M.fromList featureFiles
        Just model <- Aeson.decode <$> BSL.readFile modelFile
        when (not $ S.null $ M.keysSet (modelWeights model) `S.difference` M.keysSet runFiles) $
            fail "bad features"
        let features = toDocFeatures (M.keysSet $ modelWeights model) runFiles
            queries :: M.Map Run.QueryId [(QRel.DocumentName, Features)]
            queries = M.fromListWith (<>)
                      [ (queryId, [(doc, fs)])
                      | ((queryId, doc), fs) <- M.assocs features
                      ]

            rankings :: M.Map Run.QueryId (Ranking QRel.DocumentName)
            rankings = fmap (rerank (toWeights model)) queries

        print rankings
