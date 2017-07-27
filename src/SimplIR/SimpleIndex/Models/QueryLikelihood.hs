{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.SimpleIndex.Models.QueryLikelihood where

import Data.Hashable
import Numeric.Log

import SimplIR.SimpleIndex
import qualified SimplIR.RetrievalModels.QueryLikelihood as QL
import SimplIR.RetrievalModels.CorpusStats

queryLikelihood :: forall term doc. (Eq term, Hashable term)
                => Smoothing
                -> RetrievalModel term doc Int
queryLikelihood smoothing stats = \queryTerms _doc docLength postings ->
    QL.queryLikelihood smoothing' (map (\t -> (t,1)) queryTerms)
                       docLength (map (fmap realToFrac) postings)
  where
    bgDist = corpusTermFrequency stats
    smoothing' :: QL.Smoothing term
    smoothing' = case smoothing of
                   NoSmoothing         -> QL.NoSmoothing
                   Dirichlet mu        -> QL.Dirichlet mu bgDist
                   Laplace             -> QL.Laplace
                   JelinekMercer alpha -> QL.JelinekMercer alpha bgDist

-- | Smoothing method.
--
-- See 'QL.Smoothing' for details.
data Smoothing
    = NoSmoothing
    | Dirichlet !(Log Double)
    | Laplace
    | JelinekMercer !(Log Double)