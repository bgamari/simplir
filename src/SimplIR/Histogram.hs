{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DataKinds #-}

module SimplIR.Histogram
    ( Histogram
    , histogram
    , histogramFoldable
    , binCounts
      -- * Binning strategies
    , Binning
      -- ** Bounded
    , BoundedBin(..), bounded
      -- ** Linear
    , linearBinning
      -- ** Logarithmic
    , logBinning
    ) where

import qualified Control.Foldl as Foldl
import Data.Profunctor
import Control.Monad.ST
import Data.Bifunctor
import Data.Ix
import Data.Proxy
import GHC.TypeNats
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Indexed as VI
import qualified Data.Vector.Indexed.Mutable as VIM

data Histogram n bin a = Histogram (Binning n a bin) (VI.Vector VU.Vector (BinIdx n) Word)

newtype BinIdx (n :: Nat) = BinIdx Int
                 deriving (Show, Eq, Ord, Enum, Ix)

instance KnownNat n => Bounded (BinIdx n) where
    minBound = BinIdx 0
    maxBound = BinIdx $ fromIntegral $ natVal (Proxy @n) - 1

data Binning n a bin = Binning { toBin :: a -> Maybe (BinIdx n)
                               , fromIndex :: BinIdx n -> bin
                               }
                     deriving (Functor)

instance Profunctor (Binning n) where
    lmap f (Binning g h) = Binning (g . f) h
    rmap = fmap

data BoundedBin a = TooLow
                  | InBin a
                  | TooHigh

bounded :: forall n a bin. (KnownNat n, Ord a)
        => (a, a)
        -> ((a, a) -> Binning n a bin)
        -> Binning (n+2) a (BoundedBin bin)
bounded (l,u) f =
    Binning { toBin = \x -> if | x < l -> Just (BinIdx 0)
                               | x > u -> Just tooHighBin
                               | otherwise -> case toBin binning x of
                                                Just (BinIdx n) -> Just (BinIdx (n+1))
                                                Nothing         -> error "SimplIR.Histogram.bounded: Uh oh"
            , fromIndex = \case BinIdx 0 -> TooLow
                                idx | idx == tooHighBin-> TooHigh
                                BinIdx n -> InBin $ fromIndex binning (BinIdx (n-1))
            }
  where
    tooHighBin = BinIdx (fromIntegral binCount -1)
    binning = f (l,u)
    binCount = natVal (Proxy @n)
{-# INLINEABLE bounded #-}

linearBinning :: forall n a. (KnownNat n, RealFrac a)
              => (a, a) -> Binning (n+2) a (a, a)
linearBinning (l,u) =
    Binning { toBin = \x -> if | x < l     -> Nothing
                               | x > u     -> Nothing
                               | otherwise -> Just $ BinIdx $ truncate $ (x - l) / d
            , fromIndex = \(BinIdx n) -> (l + (d * realToFrac n), l + (d * realToFrac (n+1)))
            }
  where
    d = (u - l) / realToFrac binCount
    binCount = natVal (Proxy @n)
{-# INLINEABLE linearBinning #-}

logBinning :: forall n a. (KnownNat n, RealFloat a)
           => (a, a) -> Binning (n+2) a (a, a)
logBinning (l,u) = dimap log f $ linearBinning (l', u')
  where
    f (a,b) = (exp a, exp b)
    l' = log l
    u' = log u

histogram :: forall n a bin s. (KnownNat n)
          => Binning n a bin
          -> Foldl.FoldM (ST s) a (Histogram n bin a)
histogram binning = Foldl.FoldM step begin end
  where
    binCount = natVal (Proxy @n)
    begin = VIM.new (BinIdx 0, BinIdx $ fromIntegral binCount)
    end acc = do v <- VI.freeze acc
                 return $ Histogram binning v
    step acc x = case toBin binning x of
                   Just n  -> do VIM.modify acc (+1) n
                                 return acc
                   Nothing -> return acc

histogramFoldable :: forall n a bin f. (KnownNat n, Foldable f)
                  => Binning n a bin
                  -> f a
                  -> Histogram n bin a
histogramFoldable binning xs = runST $ Foldl.foldM (histogram binning) xs

binCounts :: forall n bin a. (KnownNat n) => Histogram n bin a -> [(bin, Word)]
binCounts (Histogram binning v) =
    map (first $ fromIndex binning) $ VI.assocs v
