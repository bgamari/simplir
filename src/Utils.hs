{-# LANGUAGE RankNTypes #-}
module Utils where

import qualified Control.Foldl as Foldl
import Pipes
import qualified Data.Map as M
import qualified Pipes.Prelude as P.P

-- | A variant of 'cat' with the type parameters rearranged for convenient use
-- with @TypeApplications@.
cat' :: forall a m r. Monad m => Pipe a a m r
cat' = cat

foldProducer :: Monad m => Foldl.FoldM m a b -> Producer a m () -> m b
foldProducer (Foldl.FoldM step initial extract) =
    P.P.foldM step initial extract

-- | Fold over chunks of the output of a 'Producer' with the given 'FoldM',
-- emitting the result of each.
foldChunks :: Monad m => Int -> Foldl.FoldM m a b -> Producer a m () -> Producer b m ()
foldChunks chunkSize (Foldl.FoldM step initial extract) = start
  where
    start prod = do
        acc <- lift initial
        go chunkSize acc prod

    go 0 acc prod = do
        lift (extract acc) >>= yield
        start prod
    go n acc prod = do
        mx <- lift $ next prod
        case mx of
          Right (x, prod') -> do
              acc' <- lift $ step acc x
              go (n-1 :: Int) acc' prod'
          Left r -> lift (extract acc) >>= yield

-- | Zip the elements coming down a 'Pipe' with elements of a list. Fails if the
-- list runs out of elements.
zipWithList :: Monad m => [i] -> Pipe a (i,a) m r
zipWithList = go
  where
    go []     = error "zipWithList: Reached end of list"
    go (i:is) = do
        x <- await
        yield (i, x)
        go is

-- | Fold over a set of 'M.Map's, monoidally merging duplicates.
mconcatMaps :: (Ord k, Monoid a) => Foldl.Fold (M.Map k a) (M.Map k a)
mconcatMaps = Foldl.Fold (M.unionWith mappend) M.empty id
