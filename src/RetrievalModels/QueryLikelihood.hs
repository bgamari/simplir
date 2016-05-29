module RetrievalModels.QueryLikelihood where

import Data.Foldable
import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromMaybe)
import Numeric.Log hiding (sum)
import Types

type Score = Log Double
type TermProb = Log Double

data Smoothing
    = NoSmoothing
    | Dirichlet !(Log Double) (Term -> Log Double)
      -- ^ Given by Dirichlet factor $mu$ and background
      -- term probability. Values around the average
      -- document length or of order 100 are good guesses.
    | Laplace
    -- | Mercer

-- | Score a document under the query likelihood model.
queryLikelihood :: Smoothing                -- ^ what smoothing to apply
                -> [(Term, Int)]            -- ^ the query's term frequencies
                -> DocumentLength           -- ^ the length of the document being scored
                -> [(Term, Int)]            -- ^ the document's term frequencies
                -> Score                    -- ^ the score under the query likelihood model
queryLikelihood smoothing query = \(DocLength docLen) docTerms ->
    let denom = case smoothing of
                  NoSmoothing            -> realToFrac docLen
                  Dirichlet mu _termProb -> realToFrac docLen + mu
                  Laplace                -> realToFrac docLen + 2

        docTfs :: HM.HashMap Term Int
        docTfs = foldl' accum (fmap (const 0) queryTerms) docTerms

        accum :: HM.HashMap Term Int -> (Term, Int) -> HM.HashMap Term Int
        accum acc (term, tf) = HM.adjust (+tf) term acc
    in product [ (num / denom)^(queryTf term)
               | (term, tf) <- HM.toList docTfs
               , let num = case smoothing of
                             NoSmoothing            -> realToFrac tf
                             Dirichlet mu termProb  -> realToFrac tf + mu * termProb term
                             Laplace                -> realToFrac tf + 1
               ]
  where
    queryTerms :: HM.HashMap Term Int
    queryTerms = HM.fromList query

    queryTf :: Term -> Int
    queryTf term = fromMaybe 0 $ HM.lookup term queryTerms

