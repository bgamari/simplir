module Control.Foldl.Map where

import Data.Maybe
import qualified Control.Foldl as Foldl
import qualified Data.Map as M

multiFold :: (Ord k) => Foldl.Fold a b -> Foldl.Fold (k,a) (M.Map k b)
multiFold (Foldl.Fold step initial extract) = Foldl.Fold step' M.empty (fmap extract)
  where
    step' acc (k,x) = M.alter (Just . (`step` x) . fromMaybe initial) k acc

multiFoldM :: (Ord k, Monad m) => Foldl.FoldM m a b -> Foldl.FoldM m (k,a) (M.Map k b)
multiFoldM (Foldl.FoldM step initial extract) = Foldl.FoldM step' initial' extract'
  where
    initial' = pure M.empty
    extract' = traverse extract
    step' acc (k,x) = do
        acc0 <- case M.lookup k acc of
                  Nothing -> initial
                  Just acc0 -> pure acc0
        acc' <- step acc0 x
        return $ M.insert k acc' acc

-- | Fold over a set of 'M.Map's, monoidally merging duplicates.
mconcatMaps :: (Ord k, Monoid a) => Foldl.Fold (M.Map k a) (M.Map k a)
mconcatMaps = Foldl.Fold (M.unionWith mappend) M.empty id
