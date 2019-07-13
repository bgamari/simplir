module Control.Foldl.HashMap where

import Data.Hashable
import Data.Maybe
import qualified Control.Foldl as Foldl
import qualified Data.HashMap.Strict as HM

multiFold :: (Hashable k, Eq k) => Foldl.Fold a b -> Foldl.Fold (k,a) (HM.HashMap k b)
multiFold (Foldl.Fold step initial extract) = Foldl.Fold step' HM.empty (fmap extract)
  where
    step' acc (k,x) = HM.alter (Just . (`step` x) . fromMaybe initial) k acc

multiFoldM :: (Hashable k, Eq k, Monad m) => Foldl.FoldM m a b -> Foldl.FoldM m (k,a) (HM.HashMap k b)
multiFoldM (Foldl.FoldM step initial extract) = Foldl.FoldM step' initial' extract'
  where
    initial' = pure HM.empty
    extract' = traverse extract
    step' acc (k,x) = do
        acc0 <- case HM.lookup k acc of
                  Nothing -> initial
                  Just acc0 -> pure acc0
        acc' <- step acc0 x
        return $ HM.insert k acc' acc

-- | Fold over a set of 'HHM.HashMap's, monoidally merging duplicates.
mconcatMaps :: (Hashable k, Eq k, Monoid a) => Foldl.Fold (HM.HashMap k a) (HM.HashMap k a)
mconcatMaps = Foldl.Fold (HM.unionWith mappend) HM.empty id
