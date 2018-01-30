{-# LANGUAGE DeriveFunctor #-}

-- | Primitives for computing various simple statistics
module SimplIR.Statistics
    ( -- * Means
      Mean
    , one
    , several
      -- ** Accessing
    , getMean
    ) where

import Data.Semigroup

-- | Compute a mean.
--
-- For instance,
--
    -- >>> getMean $ foldMap one [1, 4, 6]
--
data Mean a = Mean !Int !a
            deriving (Functor)

instance Num a => Monoid (Mean a) where
    mempty = Mean 0 0
    mappend = (<>)

instance Num a => Semigroup (Mean a) where
    Mean a m <> Mean b n = Mean (a+b) (m+n)

-- | A single sample in a mean.
one :: a -> Mean a
one = Mean 1

-- | Several samples in a mean.
several :: Int -> a -> Mean a
several = Mean

-- | Compute the mean
getMean :: Fractional a => Mean a -> a
getMean (Mean n s) = s / realToFrac n
