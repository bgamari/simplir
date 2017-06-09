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
      --
      -- Note that in the event that a query term has zero background
      -- probability you may see infinite scores. A common hack to avoid this is
      -- to assume that the collection frequency of such terms is somewhere
      -- between 0 and 1.
    | Laplace
    | JelinekMercer !(Log Double) (Distribution term)
      -- ^ Jelinek-Mercer smoothing.

-- | Score a term
score ::
         Smoothing term
      -> DocumentLength -- ^ the length of the document which we are scoring
      -> term           -- ^ the term
      -> Double         -- ^ how many times does it occur in the document?
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
                -> [(term, Double)]         -- ^ the query's term frequencies
                -> DocumentLength           -- ^ the length of the document being scored
                -> [(term, Double)]         -- ^ the document's term frequencies
                -> Score                    -- ^ the score under the query likelihood model
queryLikelihood smoothing query = \docLen docTerms ->
    let docTfs :: HM.HashMap term Double
        docTfs = foldl' accum (fmap (const 0) queryTerms) docTerms

        accum :: HM.HashMap term Double -> (term, Double) -> HM.HashMap term Double
        accum qacc (dterm, dtf) = HM.adjust (+dtf) dterm qacc
    in product [ (score smoothing docLen qterm dtf)**(realToFrac qTf)
               | (qterm, dtf) <- HM.toList docTfs
                 -- by construction docTfs will contain only query terms
               , let Just qTf = HM.lookup qterm queryTerms
               ]
  where
    queryTerms :: HM.HashMap term Double
    queryTerms = HM.fromList query
