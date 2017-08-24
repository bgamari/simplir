{-# LANGUAGE RecordWildCards #-}

-- | Okapi BM25
module SimplIR.RetrievalModels.BM25 where

import Data.Maybe
import Data.Hashable
import Numeric.Log hiding (sum)
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import SimplIR.RetrievalModels.CorpusStats
import SimplIR.Types

type Score = Log Double

data BM25Params = BM25Params { k1 :: !(Log Double)
                             , b  :: !(Log Double)
                             }

sensibleParams :: BM25Params
sensibleParams = BM25Params 1.2 0.75

-- | This is Lucene's variant on Okapi BM-25. It ignores query term frequency
-- and document length bias.
--
-- We use the SMART probidf version of idf (with offset of 0.5 to avoid division
-- by zero).
bm25 :: (Eq term, Hashable term)
     => BM25Params -> CorpusStats term
     -> HS.HashSet term -> DocumentLength -> HM.HashMap term TermFreq -> Score
bm25 params stats queryTerms docLen terms =
    sum $ map (uncurry $ bm25Term params stats docLen)
    $ filter (\(term, _) -> term `HS.member` queryTerms)
    $ HM.toList terms
{-# INLINEABLE bm25 #-}

bm25Term :: (Eq term, Hashable term)
         => BM25Params -> CorpusStats term
         -> DocumentLength -> term -> TermFreq -> Score
bm25Term params CorpusStats{..} docLen term tf =
    bm25Term' params corpusDocCount corpusTokenCount docLen termStats tf
  where
    termStats = fromMaybe mempty $ HM.lookup term corpusTerms
{-# INLINEABLE bm25Term #-}

bm25Term' :: BM25Params -> CorpusDocCount -> CorpusTokenCount
          -> DocumentLength -> TermStats -> TermFreq -> Score
bm25Term' BM25Params{..} docCount tokCount docLen TermStats{..} tf =
    idf * tf' * (k1 + 1) / (tf' + k1 * (1 - b + b * getDocumentLength docLen / avgDocLen))
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
