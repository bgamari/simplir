module AccumPostings
    ( TermPostings
    , toPostings
    , foldPostings
    ) where

import Data.Profunctor
import qualified Control.Foldl as Foldl
import           Control.Foldl.Map
import qualified Data.Map as M
import qualified Data.DList as DList
import           Data.DList (DList)

import SimplIR.Types
import SimplIR.Term

type TermPostings a = [(Term, Posting a)]

toPostings :: DocumentId -> [(Term, a)] -> TermPostings a
toPostings docId terms =
    [ (term, Posting docId pos)
    | (term, pos) <- terms
    ]

foldPostings :: Foldl.Fold (TermPostings p) (M.Map Term (DList (Posting p)))
foldPostings = Foldl.handles traverse $ lmap (fmap DList.singleton) $ multiFold Foldl.mconcat
