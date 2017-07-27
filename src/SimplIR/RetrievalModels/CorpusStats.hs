{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.RetrievalModels.CorpusStats
    ( -- * Types
      CorpusDocCount
    , CorpusTokenCount
    , TermStats(..)
    , TermFreq
    , CorpusStats(..)

     -- * Background statistics
    , lookupTermStats
    , corpusTermFrequency

      -- * Construction
    , addCorpusStats
    , documentTermStats
    ) where

import Data.Hashable
import Data.Semigroup
import Data.Profunctor
import qualified Data.HashSet as HS
import qualified Data.HashMap.Strict as HM
import qualified Control.Foldl as Foldl
import qualified Data.Binary.Serialise.CBOR as CBOR
import GHC.Generics

type CorpusDocCount = Int
type CorpusTokenCount = Int
type TermFreq = Int

data TermStats = TermStats { documentFrequency :: !Int
                           , termFrequency     :: !Int
                           }
               deriving (Show, Generic)
instance CBOR.Serialise TermStats

instance Semigroup TermStats where
    TermStats a b <> TermStats x y = TermStats (a+x) (b+y)
instance Monoid TermStats where
    mempty = TermStats 0 0
    mappend = (<>)

data CorpusStats term = CorpusStats { corpusTerms      :: !(HM.HashMap term TermStats)
                                      -- ^ per-term usage statistics
                                    , corpusDocCount   :: !CorpusDocCount
                                      -- ^ total document count
                                    , corpusTokenCount :: !CorpusTokenCount
                                      -- ^ total token count
                                    }
                      deriving (Generic)
instance (Hashable term, Eq term, CBOR.Serialise term) => CBOR.Serialise (CorpusStats term)

lookupTermStats :: (Eq term, Hashable term)
                => CorpusStats term -> term -> Maybe TermStats
lookupTermStats cs t = HM.lookup t (corpusTerms cs)

corpusTermFrequency :: (Eq term, Hashable term, Fractional a)
                    => CorpusStats term -> term -> a
corpusTermFrequency cs t =
    case lookupTermStats cs t of
      Nothing -> 0
      Just ts -> realToFrac (termFrequency ts) / realToFrac (corpusTokenCount cs)
{-# INLINEABLE corpusTermFrequency #-}

-- | Assumes sets cover non-overlapping sets of documents.
instance (Eq term, Hashable term) => Monoid (CorpusStats term) where
    mempty = emptyCorpusStats
    mappend = addCorpusStats
    mconcat = addManyCorpusStats

emptyCorpusStats :: (Eq term, Hashable term) => CorpusStats term
emptyCorpusStats =
    CorpusStats { corpusTerms = mempty
                , corpusDocCount = 0
                , corpusTokenCount = 0
                }

addCorpusStats :: (Hashable term, Eq term)
               => CorpusStats term -> CorpusStats term -> CorpusStats term
addCorpusStats a b =
    CorpusStats { corpusTerms = HM.unionWith mappend (corpusTerms a) (corpusTerms b)
                , corpusDocCount = corpusDocCount a + corpusDocCount b
                , corpusTokenCount = corpusTokenCount a + corpusTokenCount b
                }

addManyCorpusStats :: (Hashable term, Eq term)
                   => [CorpusStats term] -> CorpusStats term
addManyCorpusStats xs =
    CorpusStats { corpusTerms = HM.fromListWith mappend
                                $ concatMap (HM.toList . corpusTerms) xs
                , corpusDocCount = sum $ map corpusDocCount xs
                , corpusTokenCount = sum $ map corpusTokenCount xs
                }

-- | A 'Foldl.Fold' over documents (bags of words) accumulating TermStats
documentTermStats :: forall term. (Hashable term, Eq term)
                  => Maybe (HS.HashSet term) -> Foldl.Fold [term] (CorpusStats term)
documentTermStats interestingTerms =
    CorpusStats <$> termStats <*> Foldl.length <*> lmap Prelude.length Foldl.sum
  where
    accumTermStats = Foldl.Fold (HM.unionWith mappend) mempty id
    termStats = lmap toTermStats $ Foldl.handles traverse accumTermStats
    toTermStats :: [term] -> [HM.HashMap term TermStats]
    toTermStats = maybe unfilteredToTermStats filteredToTermStats interestingTerms

    filteredToTermStats :: HS.HashSet term -> [term] -> [HM.HashMap term TermStats]
    filteredToTermStats filterTerms terms =
        [ HM.singleton term (TermStats 1 n)
        | (term, n) <- HM.toList terms'
        , term `HS.member` filterTerms
        ]
      where
        terms' = HM.fromListWith (+) $ zip terms (repeat 1)

    unfilteredToTermStats :: [term] -> [HM.HashMap term TermStats]
    unfilteredToTermStats terms =
        [ HM.singleton term (TermStats 1 n)
        | (term, n) <- HM.toList terms'
        ]
      where
        terms' = HM.fromListWith (+) $ zip terms (repeat 1)
{-# INLINEABLE documentTermStats #-}
