{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module AccumPostings
    ( AccumPostingsM
    , runAccumPostingsSink
    , accumPostingsSink
    ) where

import Data.Foldable
import Data.Monoid
import qualified Data.Map as M
import qualified Data.DList as DList
import           Data.DList (DList)
import           Control.Monad.Trans.State.Strict
import           Control.Monad.IO.Class

import WarcDocSource
import Types

data AccumPostingsState p = APS { apsDocIds      :: !(M.Map DocumentId DocumentName)
                                , apsFreshDocIds :: ![DocumentId]
                                , apsPostings    :: !(M.Map Term (DList (Posting p)))
                                }

newtype AccumPostingsM p m a = APM (StateT (AccumPostingsState p) m a)
                             deriving (Functor, Applicative, Monad, MonadIO)

runAccumPostingsSink :: Monad m => AccumPostingsM p m a -> m (a, M.Map Term (DList (Posting p)))
runAccumPostingsSink (APM action) = do
    (r, s) <- runStateT action s0
    return (r, apsPostings s)
  where
    s0 = APS { apsDocIds = M.empty
             , apsFreshDocIds = [DocId i | i <- [0..]]
             , apsPostings = M.empty
             }

assignDocId :: Monad m => DocumentName -> AccumPostingsM p m DocumentId
assignDocId docName = APM $ do
    docId:rest <- apsFreshDocIds <$> get
    modify' $ \s -> s { apsFreshDocIds = rest
                      , apsDocIds = M.insert docId docName (apsDocIds s) }
    return docId

insertPostings :: (Monad m, Monoid p) => TermPostings p -> AccumPostingsM p m ()
insertPostings termPostings = APM $
    modify' $ \s -> s { apsPostings = foldl' insert (apsPostings s) termPostings }
  where
    insert :: M.Map Term (DList (Posting p)) -> (Term, Posting p) -> M.Map Term (DList (Posting p))
    insert acc (term, posting) =
        M.insertWith (<>) term (DList.singleton posting) acc

accumPostingsSink :: (Monad m, Monoid p) => DocumentSink (AccumPostingsM p m) p
accumPostingsSink = DocSink $ \docName postings -> do
    docId <- assignDocId docName
    insertPostings $ toPostings docId (M.assocs postings)

type TermPostings a = [(Term, Posting a)]

toPostings :: DocumentId -> [(Term, a)] -> TermPostings a
toPostings docId terms =
    [ (term, Posting docId pos)
    | (term, pos) <- terms
    ]
