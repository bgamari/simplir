module SimplIR.Bag
    ( Bag(..)
      -- * Construction
    , singleton
    , fromList
    , fromListNormed
    , fromWeights
      -- * Merging
    , weightedUnion
      -- * Manipulation
    , scale
    , normalize
      -- * Conversion
    , toList
    , byFrequency
    ) where

import Data.Hashable
import Data.List
import Data.Ord
import Data.Tuple
import Data.Semigroup
import qualified Data.HashMap.Strict as HM

newtype Bag weight a = Bag { unBag :: HM.HashMap a weight }

instance (Num weight, Hashable a, Eq a) => Semigroup (Bag weight a) where
    Bag a <> Bag b = Bag (HM.unionWith (+) a b)

instance (Num weight, Hashable a, Eq a) => Monoid (Bag weight a) where
    mempty = Bag mempty
    mappend = (<>)

singleton :: (Hashable a, Num weight)
          => a -> Bag weight a
singleton t = Bag $ HM.singleton t 1
{-# INLINEABLE singleton #-}

weightedUnion :: (Eq a, Hashable a, Num weight)
              => [(weight, Bag weight a)] -> Bag weight a
weightedUnion bags =
    Bag $ HM.fromListWith (+)
    [ (x, weight * w0)
    | (weight, Bag bag) <- bags
    , (x, w0) <- HM.toList bag
    ]
{-# INLINEABLE weightedUnion #-}

-- | L1 normalization
normalize :: Fractional weight
          => Bag weight a -> Bag weight a
normalize (Bag xs) = Bag $ fmap (/s) xs
  where s = sum xs
{-# INLINEABLE normalize #-}

scale :: (Num weight) => weight -> Bag weight a -> Bag weight a
scale s (Bag xs) = Bag $ fmap (s*) xs
{-# INLINEABLE scale #-}

toList :: Bag weight a -> [(weight, a)]
toList (Bag xs) = map swap $ HM.toList xs
{-# INLINEABLE toList #-}

-- | The elements of the bag, highest weight first.
byFrequency :: (Ord weight) => Bag weight a -> [(weight, a)]
byFrequency =
    sortBy (flip $ comparing fst) . map swap . HM.toList . unBag
{-# INLINEABLE byFrequency #-}

fromList :: (Hashable a, Eq a, Num weight) => [a] -> Bag weight a
fromList = fromWeights . map (\x -> (1, x))
{-# INLINEABLE fromList #-}

fromListNormed :: (Hashable a, Eq a, RealFrac weight)
               => [a] -> Bag weight a
fromListNormed xs =
    fromWeights $ map (\x -> (1 / realToFrac n, x)) xs
  where n = length xs
{-# INLINEABLE fromListNormed #-}

--{-# RULES "normalize fromList" normalize . fromList = fromListNormed #-}

fromWeights :: (Hashable a, Eq a, Num weight)
            => [(weight, a)] -> Bag weight a
fromWeights = Bag . HM.fromListWith (+) . map swap
{-# INLINEABLE fromWeights #-}
