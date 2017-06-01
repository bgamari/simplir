{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}

module SimplIR.WordEmbedding
    ( EmbeddingDim
    , WordVec(WordVec)
    , unWordVec
    , wordVecDim
    , WordEmbedding
    , SomeWordEmbedding(..)
    , someWordEmbeddingDim
    , wordEmbeddingDim
    ) where

import Data.Proxy
import Data.Ix
import Data.Semigroup
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import qualified Data.Array.Unboxed as A
import GHC.TypeLits

-- | A embedding dimension index.
newtype EmbeddingDim n = EmbeddingDim Int
                 deriving (Show, Eq, Ord, Ix)

instance KnownNat n => Bounded (EmbeddingDim n) where
    minBound = EmbeddingDim 0
    maxBound = EmbeddingDim $ fromIntegral (natVal (Proxy @n)) - 1

-- | A embedding word vector.
newtype WordVec (n :: Nat) = WordVec { unWordVec :: A.UArray (EmbeddingDim n) Float }

-- | The dimension of a word vector.
wordVecDim :: WordVec n -> Int
wordVecDim = rangeSize . A.bounds . unWordVec

bounds :: forall (n :: Nat). KnownNat n => (EmbeddingDim n, EmbeddingDim n)
bounds = (minBound, maxBound)

instance KnownNat n => Monoid (WordVec n) where
    mempty = WordVec $ A.accumArray (+) 0 (bounds @n) []
    mappend = (<>)
    mconcat xs =
        WordVec $ A.accumArray (+) 0 (bounds @n)
        [ (i, v / nWords)
        | WordVec vec <- xs
        , (i, v) <- A.assocs vec
        ]
      where
        !nWords = realToFrac $ length xs

-- | Word vector addition.
instance KnownNat n => Semigroup (WordVec n) where
    WordVec a <> WordVec b =
        WordVec $ A.listArray (bounds @n) (zipWith avg (A.elems a) (A.elems b))
      where
        avg x y = (x+y) / 2

    sconcat (x :| xs) = mconcat (x:xs)
    stimes _ x = x

-- | A embedding word embedding.
type WordEmbedding n = HM.HashMap T.Text (WordVec n)

data SomeWordEmbedding where
    SomeWordEmbedding :: KnownNat n => !(WordEmbedding n) -> SomeWordEmbedding

someWordEmbeddingDim :: SomeWordEmbedding -> Int
someWordEmbeddingDim (SomeWordEmbedding d) = wordEmbeddingDim d

-- | The dimension of a word embedding
wordEmbeddingDim :: forall n. KnownNat n => WordEmbedding n -> Int
wordEmbeddingDim _ = fromIntegral $ natVal (Proxy @n)
