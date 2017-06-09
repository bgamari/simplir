{-# LANGUAGE RecordWildCards #-}

-- | Okapi BM25
module SimplIR.RetrievalModels.BM25 where

import Data.Maybe
import Data.Hashable
import Numeric.Log hiding (sum)
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import SimplIR.RetrievalModels.CorpusStats

type Score = Log Double
type DocLength = Int

data BM25Params = BM25Params { k1 :: !(Log Double)
                             , b  :: !(Log Double)
                             }

sensibleParams :: BM25Params
sensibleParams = BM25Params 1.2 0.75

bm25 :: (Eq term, Hashable term)
     => BM25Params -> CorpusStats term
     -> HS.HashSet term -> DocLength -> HM.HashMap term TermFreq -> Score
bm25 params stats queryTerms docLen terms =
    sum $ map (uncurry $ bm25Term params stats docLen)
    $ filter (\(term, _) -> term `HS.member` queryTerms)
    $ HM.toList terms

bm25Term :: (Eq term, Hashable term)
         => BM25Params -> CorpusStats term
         -> DocLength -> term -> TermFreq -> Score
bm25Term params CorpusStats{..} docLen term tf =
    bm25Term' params corpusDocCount corpusTokenCount docLen termStats tf
  where
    termStats = fromMaybe mempty $ HM.lookup term corpusTerms
{-# INLINEABLE bm25 #-}

bm25Term' :: BM25Params -> CorpusDocCount -> CorpusTokenCount
          -> DocLength -> TermStats -> TermFreq -> Score
bm25Term' BM25Params{..} docCount tokCount docLen TermStats{..} tf =
    idf * tf' * (k1 + 1) / (tf' + k1 * (1 - b + b * realToFrac docLen / avgDocLen))
  where
    avgDocLen = realToFrac tokCount / realToFrac docCount
    tf' = realToFrac tf
    docFreq = realToFrac documentFrequency
    -- uses Lucene's BM-25 IDF
    idf = log $ 1 + realToFrac num / realToFrac denom
      where
        num, denom :: Double
        num = realToFrac docCount - docFreq + 0.5
        denom = docFreq + 0.5
