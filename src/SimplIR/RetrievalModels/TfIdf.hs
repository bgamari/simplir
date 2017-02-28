{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.RetrievalModels.TfIdf
    ( -- * Scoring
      Score
    , tfIdf
    , tfIdf'
    ) where

import Data.Maybe
import Data.Hashable
import Numeric.Log hiding (sum)
import qualified Data.HashMap.Strict as HM
import SimplIR.RetrievalModels.CorpusStats

type Score = Log Double

tfIdf :: (Eq term, Hashable term)
      => CorpusStats term -> term -> TermFreq -> Score
tfIdf stats term tf = tfIdf' (corpusDocCount stats) termStats tf
  where termStats = fromMaybe mempty $ HM.lookup term (corpusTerms stats)

tfIdf' :: CorpusDocCount -> TermStats -> TermFreq -> Score
tfIdf' docCount termStats tf =
    realToFrac tf * log (realToFrac docCount / (1 + realToFrac (documentFrequency termStats)))
