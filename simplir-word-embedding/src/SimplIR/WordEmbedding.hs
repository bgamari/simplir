{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}

module SimplIR.WordEmbedding
    ( -- * Word vectors
      EmbeddingDim
    , WordVec(WordVec)
    , unWordVec
    , generateWordVec
      -- ** Queries
    , wordVecDim
    , wordVecElems
      -- ** Operations
    , normaliseWordVec
    , normWordVec
    , scaleWordVec
    , subtractWordVec
    , dotWordVecs
    , sumWordVecs
      -- * Word embeddings
    , WordEmbedding
    , SomeWordEmbedding(..)
    , someWordEmbeddingDim
    , wordEmbeddingDim
    , embedTerms
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

-- | Normalise (in the L2 sense) a word vector.
normaliseWordVec :: WordVec n -> WordVec n
normaliseWordVec v = scaleWordVec (normWordVec v) v

-- | The L2 norm of a word vector
normWordVec :: WordVec n -> Float
normWordVec (WordVec v) = sqrt $ sum $ map squared $ A.elems v
  where squared x = x*x

-- | Scale a word vector
scaleWordVec :: Float -> WordVec n -> WordVec n
scaleWordVec s (WordVec v) = WordVec $ A.amap (* s) v

-- | The dimension of a word vector.
wordVecDim :: WordVec n -> Int
wordVecDim = rangeSize . A.bounds . unWordVec

-- | Create a word-vector by from the result of the given action at each
-- dimension. Useful for generating random word vectors.
generateWordVec :: forall n m. (Monad m, KnownNat n)
                => (EmbeddingDim n -> m Float) -> m (WordVec n)
generateWordVec f = WordVec . A.listArray (bounds @n) <$> mapM f (range $ bounds @n)

bounds :: forall (n :: Nat). KnownNat n => (EmbeddingDim n, EmbeddingDim n)
bounds = (minBound, maxBound)

-- | The sum of a set of word-vectors.
sumWordVecs :: forall n. KnownNat n => [WordVec n] -> WordVec n
sumWordVecs xs =
    WordVec $ A.accumArray (+) 0 (bounds @n)
    [ (i, v)
    | WordVec vec <- xs
    , (i, v) <- A.assocs vec
    ]

dotWordVecs :: WordVec n -> WordVec n -> Double
dotWordVecs (WordVec a) (WordVec b) =
    sum $ map realToFrac $ zipWith (*) (A.elems a) (A.elems b)

subtractWordVec :: forall n. KnownNat n => WordVec n -> WordVec n -> WordVec n
subtractWordVec (WordVec a) (WordVec b) =
    WordVec $ A.listArray (bounds @n) $ zipWith (-) (A.elems a) (A.elems b)

wordVecElems :: WordVec n -> [Float]
wordVecElems (WordVec a) = A.elems a

-- | Word vector addition.
instance KnownNat n => Monoid (WordVec n) where
    mempty = WordVec $ A.accumArray (+) 0 (bounds @n) []
    mappend = (<>)
    mconcat = sumWordVecs

-- | Word vector addition.
instance KnownNat n => Semigroup (WordVec n) where
    WordVec a <> WordVec b =
        WordVec $ A.listArray (bounds @n) (zipWith (+) (A.elems a) (A.elems b))

    sconcat (x :| xs) = sumWordVecs (x:xs)
    stimes n = scaleWordVec (realToFrac n)

-- | A embedding word embedding.
type WordEmbedding n = HM.HashMap T.Text (WordVec n)

data SomeWordEmbedding where
    SomeWordEmbedding :: KnownNat n => !(WordEmbedding n) -> SomeWordEmbedding

someWordEmbeddingDim :: SomeWordEmbedding -> Int
someWordEmbeddingDim (SomeWordEmbedding d) = wordEmbeddingDim d

-- | The dimension of a word embedding
wordEmbeddingDim :: forall n. KnownNat n => WordEmbedding n -> Int
wordEmbeddingDim _ = fromIntegral $ natVal (Proxy @n)

-- | The sum over the embeddings of a set of terms, dropping terms for which we
-- have no embedding.
embedTerms :: KnownNat n => WordEmbedding n -> [T.Text] -> WordVec n
embedTerms embedding terms =
    mconcat [ v
            | term <- terms
            , Just v <- pure $ HM.lookup term embedding ]
