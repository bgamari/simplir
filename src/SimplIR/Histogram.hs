{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DataKinds #-}

module SimplIR.Histogram
    ( histogram
    , binCounts
      -- * Binning strategies
    , Binning
      -- ** Bounded
    , BoundedBin(..), bounded
      -- ** Linear
    , linearBinning
    ) where

import qualified Control.Foldl as Foldl
import Control.Monad.ST
import Data.Bifunctor
import Data.Ix
import Data.Proxy
import GHC.TypeNats
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Indexed as VI
import qualified Data.Vector.Indexed.Mutable as VIM

data Histogram n bin a = Histogram (Binning n bin a) (VI.Vector VU.Vector (BinIdx n) Word)
newtype BinIdx (n :: Nat) = BinIdx Int
                 deriving (Show, Eq, Ord, Enum, Ix)
instance KnownNat n => Bounded (BinIdx n) where
    minBound = BinIdx 0
    maxBound = BinIdx $ fromIntegral $ natVal (Proxy @n) - 1

toIndex :: BinIdx n -> Int
toIndex (BinIdx n) = n

data Binning n bin a = Binning { toBin :: a -> Maybe (BinIdx n)
                               , fromIndex :: BinIdx n -> bin
                               }

data BoundedBin a = TooLow
                  | InBin a
                  | TooHigh

bounded :: forall n a bin. (KnownNat n, Ord a)
        => (a, a)
        -> ((a, a) -> Binning n bin a)
        -> Binning (n+2) (BoundedBin bin) a
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
              => (a, a) -> Binning (n+2) (a, a) a
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

histogram :: forall n a bin s. (KnownNat n)
          => Binning n bin a
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

binCounts :: forall n bin a. (KnownNat n) => Histogram n bin a -> [(bin, Word)]
binCounts (Histogram binning v) =
    map (first $ fromIndex binning) $ VI.assocs v
