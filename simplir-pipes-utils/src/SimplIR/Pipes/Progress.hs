module SimplIR.Pipes.Progress
   ( ProgressVar
   , newProgressVar
   , progressPipe
   , pollProgress
   ) where

import Data.IORef
import Control.Monad (forever, void)
import Control.Concurrent (forkIO, threadDelay)
import Pipes
import qualified Pipes.Prelude as PP

data ProgressVar a = ProgressVar (IORef a)

newProgressVar :: Monoid a => IO (ProgressVar a)
newProgressVar = ProgressVar <$> newIORef mempty

pollProgress :: ProgressVar a -> Float -> (a -> String) -> IO ()
pollProgress (ProgressVar var) period toMesg = void $ forkIO $ forever $ do
    x <- readIORef var
    putStrLn $ toMesg x
    threadDelay $ round $ period * 1000 * 1000

progressPipe :: (Monoid a, MonadIO m)
             => ProgressVar a -> (b -> a)
             -> Pipe b b m r
progressPipe (ProgressVar var) f = PP.mapM step
  where
    step x = do
        liftIO $ atomicModifyIORef var $ \acc -> (acc `mappend` f x, ())
        return x
