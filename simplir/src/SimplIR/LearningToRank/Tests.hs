{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE PartialTypeSignatures #-}

module SimplIR.LearningToRank.Tests (tests) where

import qualified Data.Map as M
import qualified Data.Set as S
import System.Random
import Test.Tasty
import Test.Tasty.HUnit

import qualified SimplIR.FeatureSpace as F
import SimplIR.LearningToRank

data Feat = X | Y
          deriving (Show, Ord, Eq)

tests :: TestTree
tests = testGroup "LearningToRank"
    [ testCase "naiveCoordAscent" testNaiveCoordAscent
    , testCase "coordAscent" testCoordAscent
    ]

--showWeights :: Show f => WeightVec f s -> String
--showWeights (WeightVec x) = show $ F.toList x

squared :: Num a => a -> a
squared x = x*x

testNaiveCoordAscent :: IO ()
testNaiveCoordAscent = do
    let gen0 = mkStdGen 42
    F.SomeFeatureSpace fspace <- pure $ F.mkFeatureSpace $  S.fromList [X, Y]
    let w0 = WeightVec $ F.fromList fspace [(X, 1),(Y, 2)]
        answer = F.fromList fspace [(X, 0),(Y, 10)]
        iters = naiveCoordAscent' Just obj gen0 w0

    let (_, WeightVec iter):_ = drop 30 iters
        distance = F.l2Norm $ iter F.^-^ answer

    assertBool "answer is close" $ distance < 10
    --putStrLn $ unlines $ map (show . fmap showWeights) $ take 4 $ iters

  where
    obj :: WeightVec Feat s -> Double
    obj (WeightVec w) =
            let m = M.fromList $ F.toList w
                x = m M.! X
                y = m M.! Y
            in - (squared x + squared (y-10))

testCoordAscent :: IO ()
testCoordAscent = do
    F.SomeFeatureSpace fspace <- pure $ F.mkFeatureSpace $  S.fromList [X, Y]
    let gen0 = mkStdGen 42
        feat x y = F.fromList fspace [(X,x), (Y,y)]
        metric = meanAvgPrec (const 2) Relevant
        frankings = M.singleton "q1"
            [ ("doc1", feat 1 10, Relevant)
            , ("doc2", feat 1 (-1), Relevant)
            , ("doc3", feat (-1) 10, NotRelevant)
            , ("doc4", feat (-1) (-1), NotRelevant)
            ]
        w0 = WeightVec $ feat 0 10
        iters = coordAscent metric gen0 w0 frankings

    let (_, WeightVec iter):_ = drop 30 iters
        m = M.fromList $ F.toList iter
        result = (m M.!)

    assertBool "Finds positive correlation" $ result X > 0
    assertBool "Suppresses negative correlation" $ result X > 2 * result Y
    --putStrLn $ unlines $ map (show . fmap showWeights) $ take 10 $ iters
