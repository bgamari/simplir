{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}

module SimplIR.FeatureSpace
    ( FeatureIndex
    -- * Feature spaces
    , SomeFeatureSpace(..)
    , FeatureSpace
    , mkFeatureSpace
    , dimension
    , featureNames
    , featureNameSet
    , featureIndexBounds
    , featureIndexes
    , lookupFeatureIndex
    , lookupFeatureName
    -- * Feature vectors
    , FeatureVec
    , featureSpace
    -- ** Construction
    , generate
    , repeat
    , fromList
    -- ** Destruction
    , toList
    -- ** Lookups
    , lookupIndex
    -- ** Mapping and zipping
    , map
    , zipWith
    -- ** Updates
    , modifyIndices
    , modify
    , accum
    -- ** Algebra
    , scale
    , (^+^), (^-^), (^*^), (^/^), sum, l2Norm
    , dot
    -- ** Mapping between spaces
    , project
    , equivSpace
    ) where

import Control.DeepSeq
import Control.Monad
import Control.Monad.Primitive
import Control.Monad.Trans.Except
import Control.Monad.ST
import Data.Bifunctor
import Data.Coerce
import Data.Foldable (forM_)
import Data.Ix
import Data.Maybe
import GHC.Stack
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Vector.Indexed as VI
import qualified Data.Vector.Indexed.Mutable as VIM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as V

import Prelude hiding (map, zipWith, repeat, sum)

newtype FeatureIndex s = FeatureIndex { getFeatureIndex :: Int }
                       deriving (Show, Ord, Eq, Enum, Ix)

data SomeFeatureSpace f where
    SomeFeatureSpace :: (forall s. FeatureSpace f s) -> SomeFeatureSpace f

data FeatureSpace f s where
    -- | Space to create low level feature vectors
    Space :: { fsIndexToFeature :: VI.Vector V.Vector (FeatureIndex s) f
             , fsFeatureToIndex :: M.Map f (FeatureIndex s)
             }
          -> FeatureSpace f s
    deriving (Show)

dimension :: FeatureSpace f s -> Int
dimension (Space v _) = rangeSize $ VI.bounds v

featureNames :: FeatureSpace f s -> [f]
featureNames = VI.elems . fsIndexToFeature

featureNameSet :: FeatureSpace f s -> S.Set f
featureNameSet = M.keysSet . fsFeatureToIndex

featureIndexBounds :: FeatureSpace f s -> (FeatureIndex s, FeatureIndex s)
featureIndexBounds = VI.bounds . fsIndexToFeature

featureIndexes :: FeatureSpace f s -> [FeatureIndex s]
featureIndexes = range . featureIndexBounds

lookupFeatureIndex :: (Ord f, HasCallStack) => FeatureSpace f s -> f -> Maybe (FeatureIndex s)
lookupFeatureIndex (Space _ m) = (`M.lookup` m)
{-# INLINEABLE lookupFeatureIndex #-}

lookupFeatureName :: HasCallStack => FeatureSpace f s -> FeatureIndex s -> f
lookupFeatureName (Space v _) = (v VI.!)
{-# INLINEABLE lookupFeatureName #-}

unsafeFeatureSpaceFromSorted :: (Ord f) => [f] -> FeatureSpace f s
unsafeFeatureSpaceFromSorted sorted = Space v m
  where
    m = M.fromAscList $ zip sorted (fmap FeatureIndex [0..])
    bs = (FeatureIndex 0, FeatureIndex $ M.size m - 1)
    v = VI.fromList bs $ fmap fst $ M.toAscList m

mkFeatureSpace :: (Ord f, Show f, HasCallStack)
               => S.Set f -> SomeFeatureSpace f
mkFeatureSpace fs = SomeFeatureSpace $ unsafeFeatureSpaceFromSorted $ S.toAscList fs

data FeatureVec f s a = FeatureVec { featureSpace  :: !(FeatureSpace f s)
                                   , getFeatureVec :: !(VI.Vector VU.Vector (FeatureIndex s) a)
                                   }
                      deriving (Show)
instance NFData (FeatureVec f s a) where
    rnf x = x `seq` ()

data SomeFeatureVec f a where
    SomeFeatureVec :: (forall s. FeatureVec f s a) -> SomeFeatureVec f a

lookupIndex :: (HasCallStack, VU.Unbox a)
            => FeatureVec f s a -> FeatureIndex s -> a
lookupIndex (FeatureVec _ v) = (v VI.!)
{-# INLINE lookupIndex #-}

accum :: (Ord f, VU.Unbox a) => (a -> a -> a) -> FeatureVec f s a -> [(f, a)] -> FeatureVec f s a
accum f v = accumIndex f v . fmap (first $ fromMaybe err . lookupFeatureIndex (featureSpace v))
  where
    err = error "FeatureSpace.accum: Invalid feature name"

accumIndex :: VU.Unbox a => (a -> a -> a) -> FeatureVec f s a -> [(FeatureIndex s, a)] -> FeatureVec f s a
accumIndex f (FeatureVec space v) = FeatureVec space . VI.accum f v

modify :: (Ord f, VU.Unbox a) => FeatureVec f s a -> [(f, a)] -> FeatureVec f s a
modify = accum (const id)

fromList :: (Ord f, VU.Unbox a) => FeatureSpace f s -> [(f, a)] -> FeatureVec f s a
fromList fspace xs = FeatureVec fspace $ VI.create $ do
    acc <- VIM.new (featureIndexBounds fspace)
    flag <- VIM.replicate (featureIndexBounds fspace) False
    forM_ xs $ \(f, x) -> do
        let Just i = lookupFeatureIndex fspace f
        VIM.write acc i x
        VIM.write flag i True

    flag' <- VI.freeze flag
    when (VU.or $ VI.vector flag') $ fail "failed to set all features"
    pure acc

toList :: (VU.Unbox a) => FeatureVec f s a -> [(f, a)]
toList (FeatureVec fspace v) = zip (featureNames fspace) (VI.elems v)

repeat :: (VU.Unbox a) => FeatureSpace f s -> a -> FeatureVec f s a
repeat fspace x = FeatureVec fspace $ VI.replicate (featureIndexBounds fspace) x

generateM :: (VU.Unbox a, PrimMonad m) => FeatureSpace f s -> (f -> m a) -> m (FeatureVec f s a)
generateM fspace f = FeatureVec fspace <$> VI.generateM (featureIndexBounds fspace) (f . lookupFeatureName fspace)

generate :: VU.Unbox a => FeatureSpace f s -> (f -> a) -> FeatureVec f s a
generate fspace f = FeatureVec fspace $ VI.generate (featureIndexBounds fspace) (f . lookupFeatureName fspace)

data FeatureMapping f s g where
    FeatureMapping :: FeatureSpace g s'
                   -> (forall a. FeatureVec f s a -> FeatureVec g s' a)
                   -> FeatureMapping f s g

--mapFeatures :: FeatureSpace f s -> (f -> g) -> FeatureMapping f s g
--mapFeatures (FeatureVec fspace _) = FeatureVec fspace

map :: (VU.Unbox a, VU.Unbox b)
    => (a -> b) -> FeatureVec f s a -> FeatureVec f s b
map f (FeatureVec fspace x) = FeatureVec fspace $ VI.map f x

project :: forall f s s' a. (VU.Unbox a, Ord f)
        => FeatureSpace f s -> FeatureSpace f s'
        -> Maybe (FeatureVec f s a -> FeatureVec f s' a)
project (Space _ s1) s2 =
  case mapping of
    Right x  -> Just $ \v -> map (lookupIndex v . coerce) x
    Left _err -> Nothing
  where
    mapping :: Either String (FeatureVec f s' Int)
    !mapping = runST $ runExceptT $ generateM s2 $ \f ->
      case coerce $ M.lookup f s1 of
        Just i -> pure i
        Nothing -> throwE $ "project: Feature not present"

equivSpace :: (VU.Unbox a, Eq f)
           => FeatureSpace f s -> FeatureSpace f s'
           -> Maybe (FeatureVec f s a -> FeatureVec f s' a)
equivSpace (Space s1 _) fs2@(Space s2 _)
  | VI.vector s1 == VI.vector s2  = Just $ \(FeatureVec _ v) -> FeatureVec fs2 $ VI.fromVector (featureIndexBounds fs2) (VI.vector v)
  | otherwise = Nothing

zipWith :: (VU.Unbox a, VU.Unbox b, VU.Unbox c)
        => (a -> b -> c) -> FeatureVec f s a -> FeatureVec f s b -> FeatureVec f s c
zipWith f (FeatureVec fspace v1) (FeatureVec _ v2) = FeatureVec fspace $ VI.zipWith f v1 v2


scale :: (VU.Unbox a, Num a)
      => a -> FeatureVec f s a -> FeatureVec f s a
scale x = map (*x)

(^+^), (^-^), (^*^)
    :: (VU.Unbox a, Num a)
    => FeatureVec f s a
    -> FeatureVec f s a
    -> FeatureVec f s a
(^+^) = zipWith (+)
(^-^) = zipWith (-)
(^*^) = zipWith (*)

(^/^) :: (VU.Unbox a, RealFrac a)
      => FeatureVec f s a
      -> FeatureVec f s a
      -> FeatureVec f s a
(^/^) = zipWith (/)
{-# INLINEABLE (^/^) #-}
{-# INLINEABLE (^*^) #-}
{-# INLINEABLE (^+^) #-}
{-# INLINEABLE (^-^) #-}

sum :: (VU.Unbox a, Num a) => FeatureVec f s a -> a
sum (FeatureVec _ v) = VI.sum v

dot :: (VU.Unbox a, Num a) => FeatureVec f s a -> FeatureVec f s a -> a
dot a b = sum $ a ^*^ b

l2Norm :: (VU.Unbox a, Num a) => FeatureVec f s a -> a
l2Norm = sum . map (\x -> x*x)

-- | Update the values at the given 'FeatureIndex's.
modifyIndices :: VU.Unbox a => FeatureVec f s a -> [(FeatureIndex s, a)] -> FeatureVec f s a
FeatureVec space v `modifyIndices` xs = FeatureVec space (v VI.// coerce xs)
{-# INLINE modifyIndices #-}
