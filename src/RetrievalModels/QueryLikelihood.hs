module RetrievalModels.QueryLikelihood where

import qualified Data.HashMap.Strict as HM
import Data.Maybe (fromMaybe)
import Numeric.Log hiding (sum)
import qualified Numeric.Log as Log
import Types

type Score = Log Double
type TermProb = Log Double

queryLikelihood :: [(Term, Int)]
                -> (DocumentId, DocumentLength, [(Term, Int)])
                -> (DocumentId, Score)
queryLikelihood query = \(docId, DocLength docLen, terms) ->
    (docId, Log.sum $ map (\(term, tf) -> (realToFrac tf / realToFrac docLen)^(queryTf term)) terms)
  where
    queryTerms :: HM.HashMap Term Int
    queryTerms = HM.fromList query

    queryTf :: Term -> Int
    queryTf term = fromMaybe 0 $ HM.lookup term queryTerms

