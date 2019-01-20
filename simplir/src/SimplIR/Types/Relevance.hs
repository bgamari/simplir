{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.Types.Relevance
    ( -- * Graded relevance
      GradedRelevance(..)
      -- * Binary relevance
    , IsRelevant(..)
    ) where

import Data.Hashable
import Control.DeepSeq
import GHC.Generics

-- | Graded relevance judgement
newtype GradedRelevance = GradedRelevance {unGradedRelevance:: Int}
                        deriving (Eq, Ord, Show, Hashable)

-- | Binary relevance judgement
data IsRelevant = NotRelevant | Relevant
                deriving (Ord, Eq, Show, Generic)
instance Hashable IsRelevant
instance NFData IsRelevant
