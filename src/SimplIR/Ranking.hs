{-# LANGUAGE DeriveTraversable #-}
module SimplIR.Ranking
    ( -- * Rankings
      Ranking
      -- ** Construction
    , fromList
    , fromSortedList
      -- ** Destruction
    , toSortedList
      -- ** Transformer
    , takeTop
    ) where

import Data.List
import Data.Ord
import Data.Bifunctor

-- | A sorted list of items and their scores
newtype Ranking score a = Ranking [(score, a)]
                        deriving (Show, Read, Eq, Functor, Foldable, Traversable)

instance Bifunctor Ranking where
    bimap f g (Ranking xs) = Ranking $ map (\(x,y) -> (f x, g y)) xs

fromList :: Ord score => [(score, a)] -> Ranking score a
fromList = Ranking . sortBy (comparing $ Down . fst)

fromSortedList :: [(score, a)] -> Ranking score a
fromSortedList = Ranking

toSortedList :: Ranking score a -> [(score, a)]
toSortedList (Ranking xs) = xs

takeTop :: Int -> Ranking score a -> Ranking score a
takeTop k (Ranking ranking) =
    Ranking (take k ranking)