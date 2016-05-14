module TopK where

import Data.Foldable
import qualified Control.Foldl as Fold
import qualified Data.Heap as H
import Data.Ord

topK :: Ord a => Int -> Fold.Fold a [a]
topK k = Fold.Fold step H.empty (map unDown . toList)
  where
    step acc x = H.take k $ H.insert (Down x) acc
    unDown (Down x) = x
