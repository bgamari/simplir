{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE GADTs #-}

module SimplIR.FeatureSpace
    (
    -- * Feature Spaces
      FeatureSpace, featureDimension, featureNames, mkFeatureSpace, concatSpace
    -- * Feature Vectors
    , FeatureVec, getFeatureVec, concatFeatureVec, projectFeatureVec
    , repeat, fromList, generate
    , modify, toList, mapFeatureVec
    -- ** Algebraic operations
    , aggregateWith, scaleFeatureVec, dotFeatureVecs
    , sumFeatureVecs, (^-^), (^+^), (^*^), (^/^)
    -- * Unpacking to plain vector
    , toVector
    -- * Unsafe construction
    , unsafeFeatureVecFromVector
    ) where

import Control.DeepSeq
import Control.Monad
import Data.List hiding (repeat)
import Data.Maybe
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Map.Strict as M
import GHC.Stack
import Prelude hiding (repeat)


-- Should be opaque
newtype FeatureVec f a = FeatureVec { getFeatureVec :: VU.Vector a }
  deriving (Show, NFData)

-- | It is the responsibility of the caller to guarantee that the indices
-- correspond to the feature space.
unsafeFeatureVecFromVector :: VU.Vector a -> FeatureVec f a
unsafeFeatureVecFromVector = FeatureVec

newtype FeatureIndex f = FeatureIndex Int
                       deriving (Show)

data FeatureSpace f where
    -- | Space to create low level feature vectors
    Space :: { fsOrigin :: CallStack
             , fsIndexToFeature :: V.Vector f
             , fsFeatureToIndex :: M.Map f (FeatureIndex f)
             }
          -> FeatureSpace f
    deriving (Show)

featureDimension :: FeatureSpace f -> Int
featureDimension (Space _ v _) = V.length v

featureNames :: FeatureSpace f -> [f]
featureNames (Space _ v _) = V.toList v

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

projectFeatureVec :: forall f g a. (VU.Unbox a, Ord g)
                  => FeatureSpace f -> FeatureSpace g
                  -> (f -> Maybe g)
                  -> FeatureVec f a -> FeatureVec g a
projectFeatureVec fa fb convert va
  | otherwise
  = let lookup :: g -> a
        lookup g
            | Just idx <- g `M.lookup` src_map
            = lookupIndex va idx
    in FeatureVec $ V.convert $ V.map lookup (fsIndexToFeature fb)
  where
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

lookupIndex2Name :: FeatureSpace f -> FeatureIndex f -> f
lookupIndex2Name (Space _ v _) (FeatureIndex i) = v V.! i

lookupIndex :: VU.Unbox a => FeatureVec f a -> FeatureIndex f -> a
lookupIndex (FeatureVec v) (FeatureIndex i) = v VU.! i

mapFeatureVec :: (VU.Unbox a, VU.Unbox b)
              => (a -> b) -> FeatureVec f a -> FeatureVec f b
mapFeatureVec f (FeatureVec v) = FeatureVec $ VU.map f v

concatFeatureVec :: VU.Unbox a => FeatureVec f a -> FeatureVec f' a -> FeatureVec (Either f f') a
concatFeatureVec (FeatureVec v) (FeatureVec v') = FeatureVec (v VU.++ v')

scaleFeatureVec :: (Num a, VU.Unbox a) => a -> FeatureVec f a -> FeatureVec f a
scaleFeatureVec s (FeatureVec v) = FeatureVec (VU.map (s*) v)

dotFeatureVecs :: (Num a, VU.Unbox a) => FeatureVec f a -> FeatureVec f a -> a
dotFeatureVecs (FeatureVec u) (FeatureVec v) = VU.sum (VU.zipWith (*) u v)


(^+^), (^-^), (^*^)
    :: (VU.Unbox a, Num a)
    => FeatureVec f a
    -> FeatureVec f a
    -> FeatureVec f a
(^/^) :: (VU.Unbox a, Fractional a)
      => FeatureVec f a
      -> FeatureVec f a
      -> FeatureVec f a
FeatureVec xs ^/^ FeatureVec ys = FeatureVec $ VU.zipWith (/) xs ys
FeatureVec xs ^*^ FeatureVec ys = FeatureVec $ VU.zipWith (*) xs ys
FeatureVec xs ^+^ FeatureVec ys = FeatureVec $ VU.zipWith (+) xs ys
FeatureVec xs ^-^ FeatureVec ys = FeatureVec $ VU.zipWith (-) xs ys

sumFeatureVecs :: (Num a, VU.Unbox a)
               => [FeatureVec f a] -> FeatureVec f a
sumFeatureVecs [] = error "sumFeatureVecs: empty list"
sumFeatureVecs fs = FeatureVec $ VU.create $ do
    accum <- VUM.replicate dim 0
    forM_ fs $ \(FeatureVec f) -> do
        forM_ [0..dim] $ \i -> do
            let v = f VU.! i
            VUM.modify accum (+ v) i
    return accum
  where dim = VU.length $ getFeatureVec $ head fs

generate :: (Ord f, Show f, VU.Unbox a, VU.Unbox a)
         => (f -> a) -> FeatureSpace f -> FeatureVec f a
generate f fspace =
    fromList fspace [ (feat, f feat) | feat <- featureNames fspace ]

repeat :: VU.Unbox a => FeatureSpace f -> a -> FeatureVec f a
repeat space value =
    FeatureVec $ VU.replicate (featureDimension space) value

fromList :: (Show f, Ord f, VU.Unbox a, HasCallStack)
         => FeatureSpace f -> [(f,a)] -> FeatureVec f a
fromList space xs = FeatureVec $ VU.create $ do
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
         => FeatureSpace f
         -> FeatureVec f a -- ^ default value
         -> [(f, a)]
         -> FeatureVec f a
modify space (FeatureVec def) xs =
    FeatureVec $ VU.accum (const id) def
    [ (i,x)
    | (f,x) <- xs
    , let FeatureIndex i = lookupName2Index space f
    ]

aggregateWith :: (VU.Unbox a)
              => (a -> a -> a)
              -> [FeatureVec f a]
              -> FeatureVec f a
aggregateWith aggregator vecs =
    FeatureVec                          -- rewrao
    $ foldl1' (VU.zipWith aggregator)   -- magic!
    $ fmap (\(FeatureVec v) -> v) vecs  -- unwrap


toList :: VU.Unbox a => FeatureSpace f -> FeatureVec f a -> [(f, a)]
toList feats (FeatureVec vals) =
    zip (featureNames feats) (VU.toList vals)

toVector :: FeatureVec f a -> VU.Vector a
toVector (FeatureVec v) = v
