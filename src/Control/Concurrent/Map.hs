{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Concurrent.Map (mapConcurrentlyL) where

import Control.Monad.Trans.Control
import Control.Monad.IO.Class
import Control.Monad.Catch
import Control.Concurrent.Async.Lifted
import Control.Concurrent.STM
import Control.Concurrent.STM.TSem

-- | Map concurrently with a limit to the number of concurrent workers.
mapConcurrentlyL :: forall m f a b.
                    (MonadBaseControl IO m, MonadIO m, MonadMask m, Traversable f)
                 => Int -> (a -> m b) -> f a -> m (f b)
mapConcurrentlyL n f xs = do
    sem <- liftIO $ atomically $ newTSem n
    mapConcurrently (withTSem sem . f) xs
{-# INLINEABLE mapConcurrentlyL #-}

withTSem :: (MonadMask m, MonadIO m) => TSem -> m a -> m a
withTSem sem g =
    bracket (liftIO $ atomically $ waitTSem sem)
            (\_ -> liftIO $ atomically $ signalTSem sem)
            (\_ -> g)
{-# INLINEABLE withTSem #-}
