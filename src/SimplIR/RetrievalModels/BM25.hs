{-# LANGUAGE RecordWildCards #-}

module SimplIR.RetrievalModels.BM25 where

import Data.Maybe
import Data.Hashable
import Data.Semigroup
import Data.Profunctor
import Numeric.Log hiding (sum)
import qualified Data.HashMap.Strict as HM
import Control.Foldl as Foldl
import SimplIR.RetrievalModels.CorpusStats

type Score = Log Double

data BM25Params = BM25Params { k1 :: Log Double
                             , b  :: Log Double
                             }

sensibleParams :: BM25Params
sensibleParams = BM25Params 1.1 0.75

bm25 :: (Eq term, Hashable term)
     => BM25Params -> CorpusStats term -> term -> TermFreq -> Score
bm25 params CorpusStats{..} term tf =
    bm25' params corpusDocCount corpusTokenCount termStats tf
  where termStats = fromMaybe mempty $ HM.lookup term corpusTerms

bm25' :: BM25Params -> CorpusDocCount -> CorpusTokenCount -> TermStats -> TermFreq -> Score
bm25' BM25Params{..} docCount tokCount termStats tf =
    tf' * (k1 + 1) / (tf'  + k1 * (1 - b + b * realToFrac docCount / avgDocLen))
  where
    avgDocLen = realToFrac tokCount / realToFrac docCount
    tf' = realToFrac tf
