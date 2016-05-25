module RetrievalModels.QueryLikelihood where

import Data.Foldable
import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromMaybe)
import Numeric.Log hiding (sum)
import qualified Numeric.Log as Log
import Types

type Score = Log Double
type TermProb = Log Double

data Smoothing
    = NoSmoothing
    | Dirichlet (Log Double) (Term -> Log Double)
      -- ^ Given by Dirichlet factor $mu$ and background
      -- term probability. Values around the average
      -- document length or of order 100 are good guesses.
    -- | Laplace
    -- | Mercer

queryLikelihood :: Smoothing
                -> [(Term, Int)]
                -> (doc, DocumentLength, [(Term, Int)])
                -> (doc, Score)
queryLikelihood smoothing query = \(docId, DocLength docLen, docTerms) ->
    let denom = case smoothing of
                  NoSmoothing           -> realToFrac docLen
                  Dirichlet mu termProb -> realToFrac docLen + mu

        docTfs :: HM.HashMap Term Int
        docTfs = foldl' accum (fmap (const 0) queryTerms) docTerms

        accum :: HM.HashMap Term Int -> (Term, Int) -> HM.HashMap Term Int
        accum acc (term, tf) = HM.adjust (+tf) term acc
    in (docId, product [ (num / denom)^(queryTf term)
                       | (term, tf) <- HM.toList docTfs
                       , let num = case smoothing of
                                     NoSmoothing -> realToFrac tf
                                     Dirichlet mu termProb -> realToFrac tf + mu * termProb term
                       ])
  where
    queryTerms :: HM.HashMap Term Int
    queryTerms = HM.fromList query

    queryTf :: Term -> Int
    queryTf term = fromMaybe 0 $ HM.lookup term queryTerms

