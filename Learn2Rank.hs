{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PartialTypeSignatures #-}

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
import SimplIR.LearningToRankWrapper
import qualified SimplIR.Format.TrecRunFile as Run
import qualified SimplIR.Format.QRel as QRel

main :: IO ()
main = do
    mode <- execParser $ info modes fullDesc
    mode

modes :: Parser (IO ())
modes = subparser $
        command "learn" (info learnMode mempty)
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
        let featureNames = M.keys runFiles


            docFeatures = toDocFeatures' featureNames runFiles
            franking =  augmentWithQrels qrel docFeatures
--             relevance :: M.Map (Run.QueryId, QRel.DocumentName) IsRelevant
--             relevance = M.fromList [ ((qid, doc), rel) | QRel.Entry qid doc rel <- qrel ]
--
--             franking :: M.Map Run.QueryId [(QRel.DocumentName, Features, IsRelevant)]
--             franking = M.fromListWith (++)
--                        [ (qid, [(doc, features, rel)])
--                        | ((qid, doc), features) <- M.assocs docFeatures
--                        , let rel = M.findWithDefault NotRelevant (qid, doc) relevance
--                        ]
            metric = avgMetric qrel
            (model, evalScore) = learnToRank franking featureNames metric gen0
        print evalScore
        BSL.writeFile modelFile $ Aeson.encode $ model




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
        let features = toDocFeatures' (M.keys $ modelWeights model) runFiles
            featureData :: M.Map Run.QueryId [(QRel.DocumentName, Features)]
            featureData = M.fromListWith (<>)
                      [ (queryId, [(doc, fs)])
                      | ((queryId, doc), fs) <- M.assocs features
                      ]

            rankings :: M.Map Run.QueryId (Ranking QRel.DocumentName)
--             rankings = fmap (rerank (toWeights model)) featureData
            rankings = rerankRankings model featureData
        print rankings
