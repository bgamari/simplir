module RetrievalModels.QueryLikelihood where

import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromMaybe)
import Numeric.Log hiding (sum)
import qualified Numeric.Log as Log
import Types

type Score = Log Double
type TermProb = Log Double

data Smoothing = NoSmoothing
               | Dirichlet (Log Double) (Term -> Log Double)

queryLikelihood :: Smoothing
                -> [(Term, Int)]
                -> (DocumentId, DocumentLength, [(Term, Int)])
                -> (DocumentId, Score)
queryLikelihood smoothing query = \(docId, DocLength docLen, terms) ->
    let denom = case smoothing of
                  NoSmoothing           -> realToFrac docLen
                  Dirichlet mu termProb -> realToFrac docLen + mu
    in (docId, Log.sum [ (num / denom)^(queryTf term)
                       | (term, tf) <- terms
                       , let num = case smoothing of
                                     NoSmoothing           -> realToFrac tf
                                     Dirichlet mu termProb -> realToFrac tf + mu * termProb term
                       ])
  where
    queryTerms :: HM.HashMap Term Int
    queryTerms = HM.fromList query

    queryTf :: Term -> Int
    queryTf term = fromMaybe 0 $ HM.lookup term queryTerms

