module SimplIR.RetrievalModels.QueryLikelihood
    ( Score
    , queryLikelihood
      -- * Smoothing models
    , TermProb
    , Smoothing(..)
    ) where

import Data.Foldable
import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromMaybe)
import Numeric.Log hiding (sum)
import SimplIR.Types

type Score = Log Double

-- | An oracle providing a term's probability under the background language
-- model.
type TermProb = Term -> Log Double

data Smoothing
    = NoSmoothing
    | Dirichlet !(Log Double) TermProb
      -- ^ Given by Dirichlet factor $mu$ and background
      -- term probability. Values around the average
      -- document length or of order 100 are good guesses.
    | Laplace
    | JelinekMercer !(Log Double) TermProb
      -- ^ Jelinek-Mercer smoothing.

-- | Score a term
score :: Smoothing
      -> DocumentLength -- ^ the length of the document which we are scoring
      -> Term           -- ^ the term
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
queryLikelihood :: Smoothing                -- ^ what smoothing to apply
                -> [(Term, Int)]            -- ^ the query's term frequencies
                -> DocumentLength           -- ^ the length of the document being scored
                -> [(Term, Int)]            -- ^ the document's term frequencies
                -> Score                    -- ^ the score under the query likelihood model
queryLikelihood smoothing query = \docLen docTerms ->
    let docTfs :: HM.HashMap Term Int
        docTfs = foldl' accum (fmap (const 0) queryTerms) docTerms

        accum :: HM.HashMap Term Int -> (Term, Int) -> HM.HashMap Term Int
        accum acc (term, tf) = HM.adjust (+tf) term acc
    in product [ (score smoothing docLen term tf)^(queryTf term)
               | (term, tf) <- HM.toList docTfs
               ]
  where
    queryTerms :: HM.HashMap Term Int
    queryTerms = HM.fromList query

    queryTf :: Term -> Int
    queryTf term = fromMaybe 0 $ HM.lookup term queryTerms
