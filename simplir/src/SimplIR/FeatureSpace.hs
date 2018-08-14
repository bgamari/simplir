{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}

module SimplIR.FeatureSpace
    (
    -- * Feature Spaces
      FeatureSpace, featureDimension, featureNames, featureNameSet, mkFeatureSpace, concatSpace
    -- * Feature Vectors
    , FeatureVec, featureSpace, getFeatureVec, featureVecDimension
    , concatFeatureVec, projectFeatureVec
    , repeat, fromList, generate
    , modify, toList, mapFeatureVec, modifyIndices
    -- ** Algebraic operations
    , l2Normalize, l2Norm
    , aggregateWith, scaleFeatureVec, dotFeatureVecs
    , sumFeatureVecs, (^-^), (^+^), (^*^), (^/^)
    -- * Unpacking to plain vector
    , toVector
    -- * Indexing
    , FeatureIndex
    , getFeatureIndex
    , featureIndexes
    , lookupIndex
    -- * Unsafe construction
    , unsafeFeatureVecFromVector
    ) where

import Control.DeepSeq
import Control.Monad
import Data.Coerce
import Data.List hiding (repeat)
import Data.Maybe
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import GHC.Stack
import Prelude hiding (repeat)
import Linear.Epsilon


-- TODO Should be opaque
data FeatureVec f a = FeatureVec { featureSpace :: !(FeatureSpace f)
                                 , getFeatureVec :: !(VU.Vector a) }
  deriving (Show)

instance NFData (FeatureVec f a) where
    rnf = (`seq` ())

-- | It is the responsibility of the caller to guarantee that the indices
-- correspond to the feature space.
unsafeFeatureVecFromVector :: FeatureSpace f -> VU.Vector a -> FeatureVec f a
unsafeFeatureVecFromVector = FeatureVec

newtype FeatureIndex f = FeatureIndex { getFeatureIndex :: Int }
                       deriving (Show, Eq)

data FeatureSpace f where
    -- | Space to create low level feature vectors
    Space :: { fsOrigin :: CallStack
             , fsIndexToFeature :: V.Vector f
             , fsFeatureToIndex :: M.Map f (FeatureIndex f)
             }
          -> FeatureSpace f
    deriving (Show)

instance Eq f => Eq (FeatureSpace f) where
    x == y = V.and $ V.zipWith (==) (fsIndexToFeature x) (fsIndexToFeature y)

instance NFData (FeatureSpace f) where
    rnf (Space a b c) = a `seq` b `seq` c `seq` ()

featureDimension :: FeatureSpace f -> Int
featureDimension (Space _ v _) = V.length v

featureNames :: FeatureSpace f -> [f]
featureNames (Space _ v _) = V.toList v

featureNameSet :: FeatureSpace f -> S.Set f
featureNameSet (Space _ _ m) = M.keysSet m

featureIndexes :: FeatureSpace f -> [FeatureIndex f]
featureIndexes s = map FeatureIndex [0..featureDimension s-1]

mkFeatureSpace :: (Ord f, Show f, HasCallStack)
               => [f] -> FeatureSpace f
mkFeatureSpace xs0
  | not $ null duplicates =
     error $ "SimplIR.FeatureSpace.mkFeatureSpace: Duplicate features: "++show duplicates
  | otherwise  = unsafeFeatureSpaceFromSorted sorted
  where
    duplicates = go sorted
      where go (x:y:xs)
              | x == y = x : go xs
            go (_:xs)  = go xs
            go []      = []

    sorted = sort xs0


unsafeFeatureSpaceFromSorted :: (Ord f) => [f] -> FeatureSpace f
unsafeFeatureSpaceFromSorted sorted = Space callStack v m
  where
    m = M.fromAscList $ zip sorted (map FeatureIndex [0..])
    v = V.fromList $ map fst $ M.toAscList m

concatSpace :: (Ord f, Ord f')
            => FeatureSpace f -> FeatureSpace f' -> FeatureSpace (Either f f')
concatSpace fs1 fs2 =
    unsafeFeatureSpaceFromSorted
    $ map Left (featureNames fs1) ++ map Right (featureNames fs2)

projectFeatureVec :: forall f g a. (Show f, Eq f, Show g, VU.Unbox a, Ord g)
                  => FeatureSpace g
                  -> (f -> Maybe g)
                  -> FeatureVec f a -> FeatureVec g a
projectFeatureVec fb convert va =
    let lookup :: g -> a
        lookup g
            | Just idx <- g `M.lookup` src_map
            , not $ idx `elem` featureIndexes fa
            = error $ unlines [ "projectFeatureVec: looking up missing feature " ++ show g
                              , " Contains featureNames fa= "++ show (featureNames fa)
                              , "   fb =  "++ show (featureNames fb)
                              ]
            | Just idx <- g `M.lookup` src_map
            = lookupIndex va idx

            | otherwise
            = error $ unlines [ "projectFeatureVec: Feature not present in domain"
                              , "Feature in range: " ++ show g
                              , "Domain: " ++ show (featureNames fa)
                              ]
    in FeatureVec fb $ V.convert $ V.map lookup (fsIndexToFeature fb)
  where
    fa = featureSpace va

    src_map :: M.Map g (FeatureIndex f)
    src_map = M.fromList $ mapMaybe convert' $ M.toList (fsFeatureToIndex fa)
      where convert' (f,i) | Just g <- convert f = Just (g, i)
            convert' _ = Nothing

lookupName2Index :: (Ord f, Show f, HasCallStack) => FeatureSpace f -> f -> FeatureIndex f
lookupName2Index (Space stack _ m) x =
    fromMaybe (error err) $ M.lookup x m
  where err = unlines $ ["lookupName2Index: feature not found "++ show x
                        ,"Known features: " ]
                     ++ [show (fname, fname == x)  | (fname, _fidx) <- M.toList m]
                     ++ [show (M.lookup x m)]
                     ++ ["CallStack: "++show stack ]

lookupIndex2Name :: HasCallStack => FeatureSpace f -> FeatureIndex f -> f
lookupIndex2Name (Space _ v _) (FeatureIndex i)
  | i >= V.length v = error "lookupIndex2Name"
  | otherwise= v V.! i
{-# INLINEABLE lookupIndex2Name #-}

lookupIndex :: (HasCallStack, VU.Unbox a) => FeatureVec f a -> FeatureIndex f -> a
lookupIndex (FeatureVec _ v) (FeatureIndex i)
  | i >= VU.length v = error "lookupIndex: ugh!"
  | otherwise = v `VU.unsafeIndex` i
{-# INLINE lookupIndex #-}

featureVecDimension :: VU.Unbox a => FeatureVec f a -> Int
featureVecDimension (FeatureVec _ v) = VU.length v

mapFeatureVec :: (VU.Unbox a, VU.Unbox b)
              => (a -> b) -> FeatureVec f a -> FeatureVec f b
mapFeatureVec f (FeatureVec s v) = FeatureVec s $ VU.map f v

l2Norm :: (VU.Unbox a, RealFloat a) => FeatureVec f a -> a
l2Norm (FeatureVec _ xs) = sqrt $ VU.sum $ VU.map squared xs
  where squared x = x*x
{-# INLINE l2Norm #-}

l2Normalize :: (VU.Unbox a, RealFloat a, Epsilon a, HasCallStack)
            => FeatureVec f a -> FeatureVec f a
l2Normalize f
  | nearZero norm = error "Preventing underflow in Model: L2 norm of near-null vector."
  | otherwise     = norm `scaleFeatureVec` f
  where norm = l2Norm f
{-# INLINE l2Normalize #-}

concatFeatureVec :: VU.Unbox a => FeatureSpace (Either f f') -> FeatureVec f a -> FeatureVec f' a -> FeatureVec (Either f f') a
concatFeatureVec space (FeatureVec s v) (FeatureVec s' v') = FeatureVec space (v VU.++ v')

scaleFeatureVec :: (Num a, VU.Unbox a) => a -> FeatureVec f a -> FeatureVec f a
scaleFeatureVec x (FeatureVec s v) = FeatureVec s (VU.map (x*) v)
{-# SPECIALISE scaleFeatureVec :: Double -> FeatureVec f Double -> FeatureVec f Double #-}
{-# SPECIALISE scaleFeatureVec :: Float -> FeatureVec f Float -> FeatureVec f Float #-}

dotFeatureVecs :: (Num a, VU.Unbox a) => FeatureVec f a -> FeatureVec f a -> a
dotFeatureVecs (FeatureVec _ u) (FeatureVec _ v) = VU.sum (VU.zipWith (*) u v)
{-# SPECIALISE dotFeatureVecs :: FeatureVec f Double -> FeatureVec f Double -> Double #-}
{-# SPECIALISE dotFeatureVecs :: FeatureVec f Float -> FeatureVec f Float -> Float #-}

(^+^), (^-^), (^*^)
    :: (VU.Unbox a, Num a)
    => FeatureVec f a
    -> FeatureVec f a
    -> FeatureVec f a
(^/^) :: (VU.Unbox a, Fractional a)
      => FeatureVec f a
      -> FeatureVec f a
      -> FeatureVec f a
FeatureVec sx xs ^/^ FeatureVec sy ys = FeatureVec sx $ VU.zipWith (/) xs ys
FeatureVec sx xs ^*^ FeatureVec sy ys = FeatureVec sx $ VU.zipWith (*) xs ys
FeatureVec sx xs ^+^ FeatureVec sy ys = FeatureVec sx $ VU.zipWith (+) xs ys
FeatureVec sx xs ^-^ FeatureVec sy ys = FeatureVec sx $ VU.zipWith (-) xs ys
{-# INLINEABLE (^/^) #-}
{-# INLINEABLE (^*^) #-}
{-# INLINEABLE (^+^) #-}
{-# INLINEABLE (^-^) #-}

sumFeatureVecs :: (Num a, VU.Unbox a)
               => [FeatureVec f a] -> FeatureVec f a
sumFeatureVecs [] = error "sumFeatureVecs: empty list"
sumFeatureVecs fs = FeatureVec space $ VU.create $ do
    accum <- VUM.replicate (featureDimension space) 0
    forM_ fs $ \(FeatureVec _ f) ->
        forM_ [0..featureDimension space-1] $ \i -> do
            let v = f VU.! i
            VUM.modify accum (+ v) i
    return accum
  where space = featureSpace $ head fs

generate :: (Ord f, Show f, VU.Unbox a, VU.Unbox a)
         => (f -> a) -> FeatureSpace f -> FeatureVec f a
generate f fspace =
    fromList fspace [ (feat, f feat) | feat <- featureNames fspace ]

repeat :: VU.Unbox a => FeatureSpace f -> a -> FeatureVec f a
repeat space value =
    FeatureVec space $ VU.replicate (featureDimension space) value

fromList :: (Show f, Ord f, VU.Unbox a, HasCallStack)
         => FeatureSpace f -> [(f,a)] -> FeatureVec f a
fromList space xs = FeatureVec space $ VU.create $ do
    flags <-  VUM.replicate dim False
    acc <- VUM.unsafeNew dim
    forM_ xs $ \(f,x) -> do
          let FeatureIndex i = lookupName2Index space f
          alreadySet <-  VUM.read flags i
          when alreadySet
            $ fail' $ "Feature already set: "++show f
          VUM.write flags i True
          VUM.write acc i x

    flags' <- VU.unsafeFreeze flags
    unless (VU.all id flags') $
        let missing =
              [ lookupIndex2Name space (FeatureIndex i)
              | i <- VU.toList $ VU.map fst $ VU.filter (not . snd) $ VU.indexed flags'
              ]
        in fail' $ "Missing features: "++show missing
    return acc
  where
    dim = featureDimension space
    fail' err = error $ "SimplIR.FeatureSpace.fromList: " ++ err

modify :: (VU.Unbox a, Ord f, Show f, HasCallStack)
         => FeatureVec f a -- ^ default value
         -> [(f, a)]
         -> FeatureVec f a
modify (FeatureVec space def) xs =
    FeatureVec space $ VU.accum (const id) def
    [ (i,x)
    | (f,x) <- xs
    , let FeatureIndex i = lookupName2Index space f
    ]

aggregateWith :: (VU.Unbox a)
              => (a -> a -> a)
              -> [FeatureVec f a]
              -> FeatureVec f a
aggregateWith _ [v] = v
aggregateWith aggregator vecs =
    FeatureVec space                    -- rewrao
    $ foldl1' (VU.zipWith aggregator)   -- magic!
    $ fmap (\(FeatureVec _ v) -> v) vecs  -- unwrap
  where
    space = featureSpace $ head vecs


toList :: VU.Unbox a => FeatureVec f a -> [(f, a)]
toList (FeatureVec space vals) =
    zip (featureNames space) (VU.toList vals)

toVector :: FeatureVec f a -> VU.Vector a
toVector (FeatureVec _ v) = v

-- | Update the values at the given 'FeatureIndex's.
modifyIndices :: VU.Unbox a => FeatureVec f a -> [(FeatureIndex f, a)] -> FeatureVec f a
FeatureVec space v `modifyIndices` xs = FeatureVec space (v VU.// coerce xs)
{-# INLINE modifyIndices #-}
