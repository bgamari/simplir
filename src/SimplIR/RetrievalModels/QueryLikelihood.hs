{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.RetrievalModels.QueryLikelihood
    ( Score
    , queryLikelihood
      -- * Smoothing models
    , Distribution
    , Smoothing(..)
    ) where

import Data.Foldable
import Data.Hashable
import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromMaybe)
import Numeric.Log hiding (sum)
import SimplIR.Types

type Score = Log Double

-- | An oracle providing a term's probability under the background language
-- model.
type Distribution a = a -> Log Double

data Smoothing term
    = NoSmoothing
    | Dirichlet !(Log Double) (Distribution term)
      -- ^ Given by Dirichlet factor $mu$ and background
      -- term probability. Values around the average
      -- document length or of order 100 are good guesses.
    | Laplace
    | JelinekMercer !(Log Double) (Distribution term)
      -- ^ Jelinek-Mercer smoothing.

-- | Score a term
score ::
         Smoothing term
      -> DocumentLength -- ^ the length of the document which we are scoring
      -> term           -- ^ the term
      -> Int            -- ^ how many times does it occur?
      -> Score
score smoothing (DocLength docLen) term tf =
    case smoothing of
      NoSmoothing           -> tf' / docLen'
      Dirichlet mu termProb -> (tf' + mu * termProb term) / (docLen' + mu)
      Laplace               -> (tf' + 1) / (docLen' + 2)
      JelinekMercer alpha termProb ->
          alpha * (tf' / docLen') + (1-alpha) * termProb term
  where
    tf' = realToFrac tf
    docLen' = realToFrac docLen :: Log Double

-- | Score a document under the query likelihood model.
queryLikelihood :: forall term. (Hashable term, Eq term)
                => Smoothing term           -- ^ what smoothing to apply
                -> [(term, Int)]            -- ^ the query's term frequencies
                -> DocumentLength           -- ^ the length of the document being scored
                -> [(term, Int)]            -- ^ the document's term frequencies
                -> Score                    -- ^ the score under the query likelihood model
queryLikelihood smoothing query = \docLen docTerms ->
    let docTfs :: HM.HashMap term Int
        docTfs = foldl' accum (fmap (const 0) queryTerms) docTerms

        accum :: HM.HashMap term Int -> (term, Int) -> HM.HashMap term Int
        accum acc (term, tf) = HM.adjust (+tf) term acc
    in product [ (score smoothing docLen term tf)^(queryTf term)
               | (term, tf) <- HM.toList docTfs
               ]
  where
    queryTerms :: HM.HashMap term Int
    queryTerms = HM.fromList query

    queryTf :: term -> Int
    queryTf term = fromMaybe 0 $ HM.lookup term queryTerms
