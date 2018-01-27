{-# LANGUAGE DeriveTraversable #-}
module SimplIR.Ranking
    ( -- * Rankings
      Ranking
      -- ** Construction
    , fromList
    , fromSortedList
      -- ** Destruction
    , toSortedList
    ) where

import Data.List
import Data.Ord

-- | A sorted list of items and their scores
newtype Ranking score a = Ranking [(score, a)]
                        deriving (Show, Read, Eq, Functor, Foldable, Traversable)

fromList :: Ord score => [(score, a)] -> Ranking score a
fromList = Ranking . sortBy (comparing $ Down . fst)

fromSortedList :: [(score, a)] -> Ranking score a
fromSortedList = Ranking

toSortedList :: Ranking score a -> [(score, a)]
toSortedList (Ranking xs) = xs

