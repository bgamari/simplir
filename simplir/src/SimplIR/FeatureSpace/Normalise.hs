{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.FeatureSpace.Normalise where

import SimplIR.FeatureSpace

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

data Normalisation f a = Normalisation { normFeatures   :: FeatureVec f a -> FeatureVec f a
                                       , denormFeatures :: FeatureVec f a -> FeatureVec f a
                                         -- | un-normalize feature weights (up to sort order)
                                       , denormWeights  :: FeatureVec f a -> FeatureVec f a
                                       }

zNormalizer :: (VU.Unbox a, RealFloat a)
            => [FeatureVec f a] -> Normalisation f a
zNormalizer feats =
    Normalisation
      { normFeatures   = \xs -> (xs ^-^ mean) ^/^ std'
      , denormFeatures = \xs -> (xs ^*^ std')  ^+^ mean
      , denormWeights  = \xs -> (xs ^/^ std')
      }
  where
    (mean, std) = featureMeanDev feats
    -- Ignore uniform features
    std' = mapFeatureVec f std
      where f 0 = 1
            f x = x
{-# SPECIALISE zNormalizer :: [FeatureVec f Double] -> Normalisation f Double #-}


featureMeanDev :: forall f a. (VU.Unbox a, RealFloat a)
               => [FeatureVec f a]
               -> (FeatureVec f a, FeatureVec f a)
featureMeanDev []    = error "featureMeanDev: no features"
featureMeanDev feats = (mean, std)
  where
    feats' = V.fromList feats
    !mean = meanV feats'
    !std  = mapFeatureVec sqrt $ meanV $ fmap (\xs -> mapFeatureVec squared $ xs ^-^ mean) feats'

    meanV :: V.Vector (FeatureVec f a) -> FeatureVec f a
    meanV xss = recip n `scaleFeatureVec` V.foldl1' (^+^) xss
      where n = realToFrac $ V.length xss

squared :: Num a => a -> a
squared x = x*x
