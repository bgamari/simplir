{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.RetrievalModels.TfIdf
    ( -- * Background statistics
      CorpusStats(..)
    , TermStats(..)
    , documentTermStats
      -- * Scoring
    , tfIdf
    ) where

import Data.Hashable
import Data.Semigroup
import Data.Profunctor
import Numeric.Log hiding (sum)
import qualified Data.HashMap.Strict as HM
import Control.Foldl as Foldl

type Score = Log Double
type CorpusDocCount = Int
type TermFreq = Int

data TermStats = TermStats { documentFrequency :: !Int
                           , termFrequency     :: !Int
                           }
               deriving (Show)

instance Semigroup TermStats where
    TermStats a b <> TermStats x y = TermStats (a+x) (b+y)
instance Monoid TermStats where
    mempty = TermStats 0 0
    mappend = (<>)

data CorpusStats term = CorpusStats { corpusTerms :: !(HM.HashMap term TermStats)
                                    , corpusSize  :: !CorpusDocCount
                                    }

-- | A 'Foldl.Fold' over documents (bags of words) accumulating TermStats
documentTermStats :: forall term. (Hashable term, Eq term)
                  => Foldl.Fold [term] (CorpusStats term)
documentTermStats =
    CorpusStats <$> termStats <*> Foldl.length
  where
    termStats = lmap toTermStats $ Foldl.handles traverse Foldl.mconcat
    toTermStats :: [term] -> [HM.HashMap term TermStats]
    toTermStats terms =
        [ HM.singleton term (TermStats 1 n)
        | (term, n) <- HM.toList terms'
        ]
      where
        terms' = HM.fromListWith (+) $ zip terms (repeat 1)
{-# INLINEABLE documentTermStats #-}

tfIdf :: CorpusDocCount -> TermStats -> TermFreq -> Score
tfIdf docCount termStats tf =
    realToFrac tf * log (realToFrac docCount / (1 + realToFrac (documentFrequency termStats)))
