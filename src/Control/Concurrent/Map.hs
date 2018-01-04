module Control.Concurrent.Map (mapConcurrentlyL) where

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Concurrent.STM.TSem
import Control.Exception

-- | Map concurrently with a limit to the number of concurrent workers.
mapConcurrentlyL :: Traversable f => Int -> (a -> IO b) -> f a -> IO (f b)
mapConcurrentlyL n f xs = do
    sem <- atomically $ newTSem n
    let withSem :: IO a -> IO a
        withSem g =
            bracket (atomically $ waitTSem sem)
                    (\_ -> atomically $ signalTSem sem)
                    (\_ -> g)
    mapConcurrently (withSem . f) xs
