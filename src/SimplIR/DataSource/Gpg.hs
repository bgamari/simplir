{-# LANGUAGE FlexibleContexts #-}

module SimplIR.DataSource.Gpg
    ( decrypt
    , UserId
    , encrypt
    ) where

import           Control.Monad (when)
import           System.Process
import           System.Exit
import           System.IO (hClose)

import           Control.Concurrent.Async.Lifted
import           Control.Monad.Trans.Control
import           Data.ByteString (ByteString)

import           Pipes
import           Pipes.Safe
import qualified Pipes.ByteString as P.BS

processIt :: (MonadSafe m, MonadBaseControl IO m)
          => FilePath -> [String]
          -> Producer ByteString m ()
          -> Producer ByteString m ()
processIt cmd args prod0 = do
    code <- bracket (liftIO $ createProcess cp)
            (\(Just stdin,_,_,_) -> liftIO $ hClose stdin)
            $ \(Just stdin, Just stdout, _, pid) -> do
        pusher <- lift $ async $
            (runEffect $ prod0 >-> P.BS.toHandle stdin)
            `finally`
            (liftIO $ hClose stdin)
        P.BS.fromHandle stdout
        () <- lift $ wait pusher
        liftIO $ waitForProcess pid
    when (code /= ExitSuccess) $ fail $ "gpg failed with "++show code
  where
    cp = (proc cmd args) { std_in = CreatePipe
                         , std_out = CreatePipe }

decrypt :: (MonadSafe m, MonadBaseControl IO m)
        => Producer ByteString m ()
        -> Producer ByteString m ()
decrypt = processIt "gpg2" ["--batch", "--decrypt", "--use-agent"]

type UserId = String

encrypt :: (MonadSafe m, MonadBaseControl IO m)
        => UserId    -- ^ recipient identity
        -> Producer ByteString m ()
        -> Producer ByteString m ()
encrypt recipient =
    processIt "gpg2" ["--batch", "--encrypt", "-r", recipient]
