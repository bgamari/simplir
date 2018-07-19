{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}

module SimplIR.Utils where

import Data.Monoid
import Control.Monad ((>=>))
import System.IO.Unsafe (unsafePerformIO)

import qualified Control.Foldl as Foldl
import Pipes
import qualified Pipes.Prelude as P.P
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Char8 as BS
import qualified Pipes.ByteString as P.BS

-- | A variant of 'cat' with the type parameters rearranged for convenient use
-- with @TypeApplications@.
cat' :: forall a m r. Monad m => Pipe a a m r
cat' = cat

foldProducer :: Monad m => Foldl.FoldM m a b -> Producer a m () -> m b
foldProducer (Foldl.FoldM step initial extract) =
    P.P.foldM step initial extract

foldProducer' :: Monad m => Foldl.FoldM m a b -> Producer a m r -> m (b, r)
foldProducer' (Foldl.FoldM step initial extract) =
    P.P.foldM' step initial extract

foldChunks :: (Monad m)
           => Int -> Foldl.FoldM m a b -> Producer a m () -> Producer b m ()
foldChunks size = foldChunks' (const $ Sum 1) (Sum size)
{-# INLINEABLE foldChunks #-}

-- | Fold over fixed-size chunks of the output of a 'Producer' with the given
-- 'FoldM', emitting the result from each.
foldChunks' :: (Ord size, Monoid size, Monad m)
            => (a -> size) -> size -> Foldl.FoldM m a b -> Producer a m () -> Producer b m ()
foldChunks' getSize chunkSize (Foldl.FoldM step initial extract) = start
  where
    start prod = do
        acc <- lift initial
        go mempty acc prod

    go s acc prod
      | s >= chunkSize = do
            lift (extract acc) >>= yield
            start prod
      | otherwise  = do
            mx <- lift $ next prod
            case mx of
              Right (x, prod') -> do
                  let !s' = s <> getSize x
                  acc' <- lift $ step acc x
                  go s' acc' prod'
              Left () -> lift (extract acc) >>= yield
{-# INLINEABLE foldChunks' #-}

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

-- | Stream out a JSON array.
toJsonArray :: (Monad m, Aeson.ToJSON a)
            => Producer a m () -> Producer BS.ByteString m ()
toJsonArray prod0 = yield "[" >> go0 prod0
  where
    go0 prod = do
        mx <- lift $ next prod
        case mx of
          Right (x, prod') -> yield "\n" >> P.BS.fromLazy (Aeson.encode x) >> go prod'
          Left () -> yield "]"

    go prod = do
        mx <- lift $ next prod
        case mx of
          Right (x, prod') -> yield ",\n" >> P.BS.fromLazy (Aeson.encode x) >> go prod'
          Left () -> yield "\n]"

-- | Print all items that come down a 'Pipe'.
traceP :: (MonadIO m, Show a) => Pipe a a m r
traceP = P.P.mapM (\x -> liftIO (print x) >> return x)

-- | Fold over inputs in fixed-size chunks, folding over those results.
foldChunksOfSize :: (Ord size, Monoid size, Monad m)
                 => (a -> size)
                 -> size                -- ^ Chunk size
                 -> Foldl.FoldM m a b   -- ^ "inner" fold, reducing "points"
                 -> Foldl.FoldM m b c   -- ^ "outer" fold, reducing chunks
                 -> Foldl.FoldM m a c
foldChunksOfSize pointSize n
           (Foldl.FoldM stepIn initialIn finalizeIn)
           (Foldl.FoldM stepOut initialOut finalizeOut) =
    Foldl.FoldM step initial finalize
  where
    initial = do
        sIn <- initialIn
        sOut <- initialOut
        return (mempty, sIn, sOut)
    finalize (_, sIn, sOut) = do
        sIn' <- finalizeIn sIn
        stepOut sOut sIn' >>= finalizeOut
    step (!s, !sIn, !sOut) x
      | n <= s = do
        sIn' <- finalizeIn sIn
        sOut' <- stepOut sOut sIn'
        sIn'' <- initialIn >>= flip stepIn x
        return (pointSize x, sIn'', sOut')
      | otherwise = do
        sIn' <- stepIn sIn x
        let !s' = s <> pointSize x
        return (s', sIn', sOut)
{-# INLINEABLE foldChunksOfSize #-}

foldChunksOf :: (Monad m)
             => Int                 -- ^ Chunk size
             -> Foldl.FoldM m a b   -- ^ "inner" fold, reducing "points"
             -> Foldl.FoldM m b c   -- ^ "outer" fold, reducing chunks
             -> Foldl.FoldM m a c
foldChunksOf = foldChunksOfSize (const (Sum 1)) . Sum
{-# INLINEABLE foldChunksOf #-}

statusEvery :: MonadIO m => Int -> (Int -> String) -> Pipe a a m r
statusEvery period msg = go 0 period
  where
    go !i 0 = do
        x <- await
        liftIO $ putStrLn $ msg (period*i)
        yield x
        go (i+1) period
    go i n = do
        await >>= yield
        go i (n-1)

-- | Wrap
statusList :: Int -> (Int -> String) -> [a] -> [a]
statusList period str = go 0 period
  where
    go !m 0 (x:xs) = unsafePerformIO $ do
        putStrLn $ str (m*period)
        return (x : go (m+1) period xs)
    go m n (x:xs) = x : go m (n-1) xs
    go _ _ []     = []

zipFoldM :: forall i m a b. Monad m
         => i -> (i -> i)
         -> Foldl.FoldM m (i, a) b
         -> Foldl.FoldM m a b
zipFoldM idx0 succ' (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step initial extract
  where
    initial = do s <- initial0
                 return (idx0, s)
    extract = extract0 . snd
    step (!idx, s) x = do
        s' <- step0 s (idx, x)
        return (succ' idx, s')
{-# INLINEABLE zipFoldM #-}

zipFold :: forall i a b.
           i -> (i -> i)
        -> Foldl.Fold (i, a) b
        -> Foldl.Fold a b
zipFold idx0 succ' (Foldl.Fold step0 initial0 extract0) =
    Foldl.Fold step initial extract
  where
    initial = (idx0, initial0)
    extract = extract0 . snd
    step (!idx, s) x =
        let s' = step0 s (idx, x)
        in (succ' idx, s')
{-# INLINEABLE zipFold #-}

premapM' :: Monad m
         => (a -> m b)
         -> Foldl.FoldM m b c
         -> Foldl.FoldM m a c
premapM' f (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step initial0 extract0
  where
    step s x = f x >>= step0 s
{-# INLINEABLE premapM' #-}

postmapM' :: Monad m
          => (b -> m c)
          -> Foldl.FoldM m a b
          -> Foldl.FoldM m a c
postmapM' f (Foldl.FoldM step0 initial0 extract0) =
    Foldl.FoldM step0 initial0 (extract0 >=> f)
{-# INLINEABLE postmapM' #-}
