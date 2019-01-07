{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.Ranking
    ( -- * Rankings
      Ranking
      -- ** Construction
    , fromList
    , fromSortedList
      -- ** Destruction
    , toSortedList
      -- ** Transformation
    , takeTop
    , mapMaybe
    , filter
    ) where

import Control.DeepSeq
import qualified Data.Maybe as Maybe
import qualified Data.List as List
import Data.Ord
import Data.Bifunctor
import Prelude hiding (filter)

-- | A sorted list of items and their scores
newtype Ranking score a = Ranking [(score, a)]
                        deriving (Show, Read, Eq, Functor, Foldable, Traversable, NFData)

instance Bifunctor Ranking where
    bimap f g (Ranking xs) = Ranking $ map (\(x,y) -> (f x, g y)) xs

fromList :: Ord score => [(score, a)] -> Ranking score a
fromList = Ranking . List.sortBy (comparing $ Down . fst)

fromSortedList :: [(score, a)] -> Ranking score a
fromSortedList = Ranking

toSortedList :: Ranking score a -> [(score, a)]
toSortedList (Ranking xs) = xs

takeTop :: Int -> Ranking score a -> Ranking score a
takeTop k (Ranking ranking) =
    Ranking (take k ranking)

filter :: (a -> Bool) -> Ranking score a -> Ranking score a
filter f (Ranking xs) = Ranking $ List.filter (f . snd) xs

mapMaybe :: (a -> Maybe b) -> Ranking score a -> Ranking score b
mapMaybe f (Ranking xs) = Ranking $ Maybe.mapMaybe g xs
  where g (s,x) = fmap (\y -> (s,y)) (f x)
