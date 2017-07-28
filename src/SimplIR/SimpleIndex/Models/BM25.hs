module SimplIR.SimpleIndex.Models.BM25
    ( BM25.BM25Params(..)
    , bm25
    , BM25.sensibleParams
    ) where

import Data.Hashable
import qualified Data.HashSet as HS
import qualified Data.HashMap.Strict as HM

import SimplIR.SimpleIndex
import qualified SimplIR.RetrievalModels.BM25 as BM25

bm25 :: (Eq term, Hashable term)
     => BM25.BM25Params
     -> RetrievalModel term doc Int
bm25 params stats = \queryTerms ->
    let queryTerms' = HS.fromList queryTerms
    in \_doc docLength postings ->
          let termFreqs = HM.fromListWith (+) postings
          in BM25.bm25 params stats queryTerms' docLength termFreqs