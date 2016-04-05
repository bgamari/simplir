{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module AccumPostings
    ( TermPostings
    , toPostings
    , foldPostings
    ) where

import Data.Foldable
import Data.Monoid
import qualified Control.Foldl as Foldl
import qualified Data.Map as M
import qualified Data.DList as DList
import           Data.DList (DList)
import           Control.Monad.Trans.State.Strict
import           Control.Monad.IO.Class

import WarcDocSource
import Types

type TermPostings a = [(Term, Posting a)]

toPostings :: DocumentId -> [(Term, a)] -> TermPostings a
toPostings docId terms =
    [ (term, Posting docId pos)
    | (term, pos) <- terms
    ]

foldPostings :: Foldl.Fold (TermPostings p) (M.Map Term (DList (Posting p)))
foldPostings = Foldl.Fold step initial extract
  where
    initial = M.empty
    extract = id

    step acc postings =
        foldl' insert acc postings
      where
        insert :: M.Map Term (DList (Posting p))
               -> (Term, Posting p)
               -> M.Map Term (DList (Posting p))
        insert acc (term, posting) =
            M.insertWith (<>) term (DList.singleton posting) acc
