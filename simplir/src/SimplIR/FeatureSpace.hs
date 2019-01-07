{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
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
    -- ** Construction
    , mkFeatureSpace
    , eitherSpaces
    , concatSpaces
    -- ** Queries
    , dimension
    , featureNames
    , featureNameSet
    , featureIndexBounds
    , featureIndexes
    , lookupFeatureIndex
    , lookupFeatureName
    -- ** Manipulating feature spaces
    , mapFeatureNames
    -- * Feature vectors
    , FeatureVec
    , featureSpace
    -- ** Construction
    , generate
    , repeat
    , fromList
    , aggregateWith
    -- *** Stacking
    , Stack
    , FeatureStack(..)
    , stack
    -- ** Destruction
    , toList
    -- ** Lookups
    , lookup
    , lookupIndex
    -- ** Mapping and zipping
    , map
    , zipWith
    -- *** Generalized mapping
    , FeatureMapping(..)
    , mapFeatures
    , FeatureMappingInto(..)
    , mapFeaturesInto
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
import Control.Monad hiding (fail)
import Control.Monad.Fail
import Control.Monad.Primitive
import Control.Monad.Trans.Except
import Control.Monad.ST
import Data.Bifunctor
import Data.Coerce
import Data.Foldable (forM_)
import Data.Kind
import Data.Ix
import qualified Data.List.NonEmpty as NE
import Data.Tuple
import Data.Maybe
import GHC.Stack
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Vector.Indexed as VI
import qualified Data.Vector.Indexed.Mutable as VIM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as V

import Prelude hiding (map, zipWith, repeat, sum, lookup, fail)

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

data Stack (ss :: [Type])

mapFeatureNames :: Ord g => (f -> g) -> FeatureSpace f s -> FeatureSpace g s
mapFeatureNames f (Space a b) =
    Space { fsIndexToFeature = VI.map f a
          , fsFeatureToIndex = M.fromList $ fmap (first f) $ M.toList b
          }

eitherSpaces :: (Ord g, Ord f)
             => FeatureSpace f s -> FeatureSpace g s' -> FeatureSpace (Either f g) (Stack '[s, s'])
eitherSpaces s1 s2 = concatSpaces (mapFeatureNames Left s1) (mapFeatureNames Right s2)

concatSpaces :: Ord f => FeatureSpace f s -> FeatureSpace f s' -> FeatureSpace f (Stack '[s, s'])
concatSpaces s1 s2 =
    Space { fsIndexToFeature = VI.fromVector bnds
                               $ coerce (VI.vector $ fsIndexToFeature s1) <> coerce (VI.vector $ fsIndexToFeature s2)
          , fsFeatureToIndex = coerce (fsFeatureToIndex s1) <> fmap bump (fsFeatureToIndex s2)
          }
  where
    bump (FeatureIndex i) = FeatureIndex (i + dimension s1)
    dim = dimension s1 + dimension s2
    bnds = (FeatureIndex 0, FeatureIndex (dim - 1))

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

-- | Vector addition.
instance (Num a, VU.Unbox a) => Semigroup (FeatureVec f s a) where
    (<>) = (^+^)

lookup :: (HasCallStack, VU.Unbox a, Ord f)
       => FeatureVec f s a -> f -> Maybe a
lookup v f = lookupIndex v <$> lookupFeatureIndex (featureSpace v) f
{-# INLINE lookup #-}

lookupIndex :: (HasCallStack, VU.Unbox a)
            => FeatureVec f s a -> FeatureIndex s -> a
lookupIndex (FeatureVec _ v) = (v VI.!)
{-# INLINE lookupIndex #-}

aggregateWith :: (Ord f, VU.Unbox a) => (a -> a -> a) -> NE.NonEmpty (FeatureVec f s a) -> FeatureVec f s a
aggregateWith f xs@(x NE.:| _) = FeatureVec (featureSpace x) $ VI.zipManyWith f (fmap getFeatureVec xs)

accum :: (Ord f, VU.Unbox a) => (a -> a -> a) -> FeatureVec f s a -> [(f, a)] -> FeatureVec f s a
accum f v = accumIndex f v . fmap (first $ fromMaybe err . lookupFeatureIndex (featureSpace v))
  where
    err = error "FeatureSpace.accum: Invalid feature name"

accumIndex :: VU.Unbox a => (a -> a -> a) -> FeatureVec f s a -> [(FeatureIndex s, a)] -> FeatureVec f s a
accumIndex f (FeatureVec space v) = FeatureVec space . VI.accum f v

modify :: (Ord f, VU.Unbox a) => FeatureVec f s a -> [(f, a)] -> FeatureVec f s a
modify = accum (const id)

fromList :: (Show f, Ord f, VU.Unbox a)
         => FeatureSpace f s
         -> [(f, a)]
         -> FeatureVec f s a
fromList fspace xs = either err id $ fromList' fspace xs
  where
    err missingFeatures =
        error $ "SimplIR.FeatureSpace.fromList: Missing features: "++show missingFeatures

fromList' :: (Ord f, VU.Unbox a)
          => FeatureSpace f s
          -> [(f, a)]
          -> Either (S.Set f) (FeatureVec f s a)
fromList' fspace xs = runST $ do
    acc <- VIM.new (featureIndexBounds fspace)
    flag <- VIM.replicate (featureIndexBounds fspace) False
    forM_ xs $ \(f, x) ->
        case lookupFeatureIndex fspace f of
          Just i -> do
            VIM.write acc i x
            VIM.write flag i True
          Nothing -> return ()

    flag' <- VI.unsafeFreeze flag
    if VU.and $ VI.vector flag'
        then Right . FeatureVec fspace <$> VI.unsafeFreeze acc
        else return $ Left $ S.fromList [ lookupFeatureName fspace fIdx | (fIdx, False) <- VI.assocs flag' ]

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
                   -> (forall a. VU.Unbox a => FeatureVec f s a -> FeatureVec g s' a)
                   -> FeatureMapping f s g

mapFeatures :: forall f g s. (Show g, Ord g, Ord f)
            => FeatureSpace f s
            -> (f -> Maybe g)
            -> FeatureMapping f s g
mapFeatures srcSpace f =
    case mkFeatureSpace $ S.fromList $ fmap snd pairs of
      SomeFeatureSpace (destSpace :: FeatureSpace g s') ->
        let mapVec :: forall a. VU.Unbox a => FeatureVec f s a -> FeatureVec g s' a
            mapVec v = map (\i -> v `lookupIndex` FeatureIndex i) mapIdxs

            --mapIdxs :: FeatureVec g s' (FeatureIndex s)
            mapIdxs :: FeatureVec g s' Int
            mapIdxs = fromList destSpace [ (y, i)
                                         | (x, y) <- pairs
                                         , Just (FeatureIndex i) <- pure $ lookupFeatureIndex srcSpace x
                                         ]
        in FeatureMapping destSpace mapVec
  where
    pairs :: [(f, g)]
    pairs = mapMaybe (\x -> fmap (x,) (f x)) $ featureNames srcSpace

data FeatureMappingInto f s g s'
  where
    FeatureMappingInto :: (forall a. VU.Unbox a => FeatureVec f s a -> FeatureVec g s' a)
                       -> FeatureMappingInto f s g s'

mapFeaturesInto :: forall f g s s'. (Show g, Ord g, Ord f)
                => FeatureSpace f s
                -> FeatureSpace g s'
                -> (f -> Maybe g)
                -> Maybe (FeatureMappingInto f s g s')
mapFeaturesInto srcSpace destSpace featProj
  | missingFeaturesInSourceSpace <- featureNameSet destSpace `S.difference` S.fromList (fmap snd pairs)
  , S.null missingFeaturesInSourceSpace =
      let mapVec :: forall a. VU.Unbox a => FeatureVec f s a -> FeatureVec g s' a
          mapVec v = map (\i -> v `lookupIndex` FeatureIndex i) mapIdxs

          --mapIdxs :: FeatureVec g s' (FeatureIndex s)
          mapIdxs :: FeatureVec g s' Int
          mapIdxs = fromList destSpace [ (y, i)
                                        | (x, y) <- pairs
                                        , Just (FeatureIndex i) <- pure $ lookupFeatureIndex srcSpace x
                                        ]
      in Just $ FeatureMappingInto mapVec
  | otherwise = Nothing
  where
    pairs :: [(f, g)]
    pairs = mapMaybe (\x -> fmap (x,) (featProj x)) $ featureNames srcSpace

map :: (VU.Unbox a, VU.Unbox b)
    => (a -> b) -> FeatureVec f s a -> FeatureVec f s b
map f (FeatureVec fspace x) = FeatureVec fspace $ VI.map f x

project :: forall m f s s' a. (VU.Unbox a, Ord f, MonadFail m)
        => FeatureSpace f s -> FeatureSpace f s'
        -> m (FeatureVec f s a -> FeatureVec f s' a)
project (Space _ s1) s2 =
  case mapping of
    Right x  -> pure $ \v -> map (lookupIndex v . coerce) x
    Left err -> fail err
  where
    --mapping :: Either String (FeatureVec f s' (FeatureIndex s))
    mapping :: Either String (FeatureVec f s' Int)
    !mapping = runST $ runExceptT $ generateM s2 $ \f ->
      case coerce $ M.lookup f s1 of
        Just i -> pure i
        Nothing -> throwE $ "project: Feature not present"

data FeatureStack f ss a where
    Stack :: FeatureVec f s a -> FeatureStack f ss a -> FeatureStack f (s ': ss) a
    StackNil :: FeatureStack f '[] a

infixr 5 `Stack`

stack :: forall f ss a. (Ord f, VU.Unbox a)
      => FeatureSpace f (Stack ss) -> FeatureStack f ss a -> FeatureVec f (Stack ss) a
stack fspace vecs =
    FeatureVec fspace $ VI.fromVector (featureIndexBounds fspace) (VU.concat $ featVecs vecs)
  where
    featVecs :: forall ss'. FeatureStack f ss' a -> [VU.Vector a]
    featVecs (Stack f rest) = VI.vector (getFeatureVec f) : featVecs rest
    featVecs StackNil = []

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

quadrance :: (VU.Unbox a, Num a) => FeatureVec f s a -> a
quadrance = sum . map (\x -> x*x)

l2Norm :: (VU.Unbox a, RealFloat a) => FeatureVec f s a -> a
l2Norm = sqrt . quadrance

-- | Update the values at the given 'FeatureIndex's.
modifyIndices :: VU.Unbox a => FeatureVec f s a -> [(FeatureIndex s, a)] -> FeatureVec f s a
FeatureVec space v `modifyIndices` xs = FeatureVec space (v VI.// coerce xs)
{-# INLINE modifyIndices #-}
