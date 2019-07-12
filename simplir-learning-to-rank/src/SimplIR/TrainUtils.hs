{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DerivingStrategies #-}

-- | Various utilities for iterative model training.
module SimplIR.TrainUtils
  ( -- * Folds
    Folds(..)
  , FoldIdx
  , mkSequentialFolds
  , kFolds
    -- * Restarts
  , Restarts(..)
  , RestartIdx
  , kFoldsAndRestarts
    -- * Mini-batching
  , miniBatched
  ) where

import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.List
import Data.List.Split
import Control.DeepSeq
import System.Random as Random

newtype Folds a = Folds { getFolds :: [a] }
                deriving (Foldable, Functor, Traversable)
                deriving newtype (NFData)

newtype FoldIdx = FoldIdx Int
                deriving (Eq, Ord, Show, Enum)

numberFolds :: Folds a -> Folds (FoldIdx, a)
numberFolds (Folds xs) = Folds $ zip [FoldIdx 0..] xs

mkSequentialFolds :: Int -> [a] -> Folds [a]
mkSequentialFolds k xs = Folds $ chunksOf foldLen xs
  where
    foldLen
      | len >= 2 * k = (len `div` k) + 1  -- usual case: prevents overpopulation of last fold, e.g. [1,2] [3,4] [5,6] [7]
      | otherwise = len `div` k  -- to prevent last folds to be empty, accept overpopulation of last fold, e.g. [1] [2] [3] [4] [5,6,7]
    len = length xs

-- | A list of restarts.
newtype Restarts a = Restarts { getRestarts :: [a] }
                   deriving (Foldable, Functor, Traversable)
                   deriving newtype (NFData)

newtype RestartIdx = RestartIdx Int
                   deriving (Eq, Ord, Show, Enum)

numberRestarts :: Restarts a -> Restarts (RestartIdx, a)
numberRestarts (Restarts xs) = Restarts $ zip [RestartIdx 0..] xs

mkRestartSeeds :: StdGen -> Restarts StdGen
mkRestartSeeds = Restarts . unfoldr (Just . Random.split)

-- r might be: [(Model, Double)]


-- TODO think about turning Fold[q] into Fold (S.Set q)

-- | Train a series of models under k-fold cross-validation.
kFolds
    :: forall q d r. (Eq q, Ord q)
    => (FoldIdx -> M.Map q d -> r)
       -- ^ a training function, producing a trained result from a
       -- set of training data. 'FoldIdx' provided for diagnostics.
    -> M.Map q d
       -- ^ training data
    -> Folds [q]
       -- ^ partitioning of queries into folds
    -> Folds (M.Map q d, r)
       -- ^ the training result and set of test data for the fold
kFolds train allData foldQueries =
    fmap trainSingleFold (numberFolds foldQueries)
  where
    trainSingleFold :: (FoldIdx, [q]) -> (M.Map q d, r)
    trainSingleFold (foldIdx, testQueries) =
      let testData :: M.Map q d
          testQueries' = S.fromList testQueries
          testData =  M.filterWithKey (\query _ -> query `S.member` testQueries') allData

          trainData :: M.Map q d
          trainData =  M.filterWithKey (\query _ -> query `S.notMember` testQueries') allData
      in (testData, train foldIdx trainData)


-- | Train a model under k-fold cross-validation with random restarts.
kFoldsAndRestarts
    ::  forall q d r. (Eq q, Ord q)
    => (FoldIdx -> RestartIdx
        -> StdGen -> M.Map q d -> r)
        -- ^ a training function, producing a trained result from a
        -- set of training data. 'FoldIdx' and 'RestartIdx' provided for
        -- diagnostics.
    -> M.Map q d
        -- ^ training data
    -> Folds [q]
        -- ^ partitioning of queries into folds
    -> StdGen
        -- ^ random generator
    -> Folds (M.Map q d, Restarts r)
        -- ^ the training results and set of test data for the fold. The
        -- 'Restarts' for each fold is infinite.
kFoldsAndRestarts train allData foldQueries gen0 =
    kFolds train' allData foldQueries
  where
    train' :: FoldIdx -> M.Map q d -> Restarts r
    train' foldIdx trainData =
        fmap (\(restartIdx, gen) -> train foldIdx restartIdx gen trainData)
             (numberRestarts gens)

    gens :: Restarts StdGen
    gens = mkRestartSeeds gen0

-- | Train a model with mini-batching.
miniBatched :: forall d model qid gen.
               (RandomGen gen, Show qid, Ord qid)
            => Int   -- ^ iterations per mini-batch
            -> Int   -- ^ mini-batch size
            -> (gen -> model -> M.Map qid d -> [model])
                     -- ^ optimiser returning a new model.
            -> gen   -- ^ random generator to use to split batches
            -> model -- ^ initial weights
            -> M.Map qid d  -- ^ training data
            -> [model]
               -- ^ list of iterates, including all steps within a mini-batch;
               -- doesn't expose the model's score since it won't be comparable
               -- across batches.
miniBatched batchSteps batchSize optimise gen00 w00 fRankings
  | M.null fRankings = error "SimplIR.TrainUtils.miniBatched: no feature rankings"
  | otherwise = go gen00 w00
  where
    nQueries = M.size fRankings

    mkBatch :: gen -> M.Map qid d
    mkBatch gen =
        M.fromList [ M.elemAt i fRankings
                   | i <- indices
                   ]
      where
        indices = map (`mod` nQueries) $ take batchSize $ Random.randoms gen

    go gen0 w0 =
        steps ++ go gen4 w1
      where
        (gen1, gen2) = Random.split gen0
        (gen3, gen4) = Random.split gen2
        batch = mkBatch gen1
        steps :: [model]
        steps = take batchSteps $ optimise gen3 w0 batch
        w1 = last steps
