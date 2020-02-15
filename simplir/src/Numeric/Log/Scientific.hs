{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Numeric.Log.Scientific where

import Data.Scientific
import Data.Aeson
import Numeric.Log
import Numeric.Log.Signed

logToScientific :: forall a. RealFloat a => Int -> Log a -> Scientific
logToScientific d (Exp l) = scientific (round m) p
  where
    m :: a
    m = 10**(l / log10 - floor' (l / log10) + realToFrac d)
    p = round $ (l - log m) / log10          -- TODO: is this rounding correct?
    log10 = log 10
    floor' :: a -> a
    floor' = realToFrac . (floor :: a -> Integer)
{-# SPECIALISE logToScientific :: Int -> Log Double -> Scientific #-}
{-# SPECIALISE logToScientific :: Int -> Log Float  -> Scientific #-}

scientificToLog :: (RealFloat a) => Scientific -> Log a
scientificToLog x = realToFrac m * Exp p'
  where
    m = coefficient x
    p = base10Exponent x
    p' = realToFrac p * log 10
{-# SPECIALISE scientificToLog :: Scientific -> Log Double #-}
{-# SPECIALISE scientificToLog :: Scientific -> Log Float #-}

instance RealFloat a => ToJSON (Log a) where
    toJSON = Number . logToScientific 14

instance (RealFloat a) => FromJSON (Log a) where
    parseJSON = withScientific "log number" $ pure . scientificToLog

instance (RealFloat a) => FromJSON (SignedLog a) where
    parseJSON = withScientific "signed log number" $ \x -> pure $
                      let fixSign (Exp l) = SLExp (x >= 0) (abs l)
                      in fixSign $ scientificToLog $ abs x

instance RealFloat a => ToJSON (SignedLog a) where
    toJSON x = Number $ fixSign $ logToScientific 14 (Exp $ lnSL x)
      where
        fixSign | signSL x  = id
                | otherwise = negate


{-
x, y, z :: Log Double
x = 40
y = Exp (-600)
z = Exp (-1500)

asObj :: ToJSON (Log a) => Log a -> Value
asObj x = object ["value" .= x]
-}
