{-# LANGUAGE RankNTypes #-}
module Utils where

import qualified Control.Foldl as Foldl
import Pipes
import qualified Pipes.Prelude as P.P

-- | A variant of 'cat' with the type parameters rearranged for convenient use
-- with @TypeApplications@.
cat' :: forall a m r. Monad m => Pipe a a m r
cat' = cat

foldProducer :: Monad m => Foldl.FoldM m a b -> Producer a m () -> m b
foldProducer (Foldl.FoldM step initial extract) =
    P.P.foldM step initial extract
