{-# LANGUAGE ScopedTypeVariables #-}

-- | Relevance model.
module SimplIR.RetrievalModels.RelevanceModel (rm1) where

import Data.Hashable
import qualified SimplIR.Bag as Bag
import Numeric.Log

type Score = Log Double
type Prob = Log Double

rm1 :: forall doc term. (Eq term, Hashable term)
    => (doc -> [term])
    -> [(Score, doc)]
    -> Bag.Bag Prob term
rm1 docContents ranking =
    let docs :: Bag.Bag Prob term
        docs = Bag.normalize $ Bag.weightedUnion $ map (fmap docBag) ranking
        docBag :: doc -> Bag.Bag Score term
        docBag = Bag.normalize . Bag.fromList . docContents
    in docs
