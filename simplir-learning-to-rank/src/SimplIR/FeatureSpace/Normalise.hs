{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module SimplIR.FeatureSpace.Normalise where

import SimplIR.FeatureSpace as F

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

data Normalisation f s a
    = Normalisation { normFeatures   :: FeatureVec f s a -> FeatureVec f s a
                    , denormFeatures :: FeatureVec f s a -> FeatureVec f s a
                      -- | un-normalize feature weights (up to sort order)
                    , denormWeights  :: FeatureVec f s a -> FeatureVec f s a
                    }

zNormalizer :: (VU.Unbox a, RealFloat a)
            => [FeatureVec f s a] -> Normalisation f s a
zNormalizer feats =
    Normalisation
      { normFeatures   = \xs -> (xs ^-^ mean) ^/^ std'
      , denormFeatures = \xs -> (xs ^*^ std')  ^+^ mean
      , denormWeights  = \xs -> (xs ^/^ std')
      }
  where
    (mean, std) = featureMeanDev feats
    -- Ignore uniform features
    std' = F.map f std
      where f 0 = 1
            f x = x
{-# SPECIALISE zNormalizer :: [FeatureVec f s Double] -> Normalisation f s Double #-}


featureMeanDev :: forall f s a. (VU.Unbox a, RealFloat a)
               => [FeatureVec f s a]
               -> (FeatureVec f s a, FeatureVec f s a)
featureMeanDev []    = error "featureMeanDev: no features"
featureMeanDev feats = (mean, std)
  where
    feats' = V.fromList feats
    !mean = meanV feats'
    !std  = F.map sqrt $ meanV $ fmap (\xs -> F.map squared $ xs ^-^ mean) feats'

    meanV :: V.Vector (FeatureVec f s a) -> FeatureVec f s a
    meanV xss = recip n `F.scale` V.foldl1' (^+^) xss
      where n = realToFrac $ V.length xss

squared :: Num a => a -> a
squared x = x*x
