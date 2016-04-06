{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module CollectPostings
    ( collectPostings
    ) where

import Data.Foldable
import Data.Monoid
import Data.Function (on)

import qualified Data.Heap as H
import qualified Data.Map.Strict as M

import Pipes

import Types

data ActivePosting m p = AP { apPosting   :: !(Posting p)
                            , apTerm      :: !Term
                            , apRemaining :: Producer (Posting p) m ()
                            }

instance Eq p => Eq (ActivePosting m p) where
    (==) = (==) `on` apPosting

instance Ord p => Ord (ActivePosting m p) where
    compare = compare `on` apPosting

collectPostings :: forall m p. (Ord p, Monad m)
                => [(Term, Producer (Posting p) m ())]
                -> Producer (DocumentId, [(Term, p)]) m ()
collectPostings = start
  where
    start prods = do
        -- request one element from each producer
        s0 <- lift $ mapM nextPosting prods
        go M.empty (fold s0)

    -- | Request a posting
    nextPosting :: (Term, Producer (Posting p) m ())
                -> m (H.Heap (ActivePosting m p))
    nextPosting (term, prod) = do
        mx <- next prod
        case mx of
            Right (posting, actives) -> return $ H.singleton $ AP posting term actives
            Left ()                  -> return H.empty

    go :: M.Map DocumentId [(Term, p)]
       -> H.Heap (ActivePosting m p)
       -> Producer (DocumentId, [(Term, p)]) m ()
    go acc actives = do
        let -- pop off minimal active postings
            (mins, actives') = takeMins actives

            -- fold them into our accumulator
            accumPosting :: M.Map DocumentId [(Term, p)]
                         -> ActivePosting m p
                         -> M.Map DocumentId [(Term, p)]
            accumPosting acc' (AP (Posting docid posting) term _) =
                M.insertWith (<>) docid [(term, posting)] acc'
            acc' = foldl' accumPosting acc mins

        -- pull in new postings
        new <- lift $ mapM (\ap -> nextPosting (apTerm ap, apRemaining ap)) mins
        finishDocs acc' (actives' <> mconcat new)

    finishDocs :: M.Map DocumentId [(Term, p)]
               -> H.Heap (ActivePosting m p)
               -> Producer (DocumentId, [(Term, p)]) m ()
    finishDocs acc actives
      | H.null actives     = mapM_ yield (M.assocs acc)
    finishDocs acc actives = do
        -- see if we have finished any documents
        let minDocId = postingDocId $ apPosting $ H.minimum actives

            finished, rest :: M.Map DocumentId [(Term, p)]
            (finished, rest) = splitRBiased minDocId acc

        mapM_ yield (M.assocs finished)
        go rest actives

splitRBiased :: Ord k => k -> M.Map k a -> (M.Map k a, M.Map k a)
splitRBiased k xs =
    case M.splitLookup k xs of
        (l, x, r)  -> (l, r <> maybe mempty (M.singleton k) x)

takeMins :: Ord a => H.Heap a -> ([a], H.Heap a)
takeMins heap =
    let (mins, rest) = H.span (== H.minimum heap) heap
    in (H.toUnsortedList mins, rest)

tests :: Monad m => [(Term, Producer (Posting ()) m ())]
tests =
    [ ("cat",    each [p 1, p 3, p 4, p 7])
    , ("turtle", each [p 2, p 3, p 7, p 9])
    , ("dog",    each [p 2000, p 3000])
    ]
  where
    p :: Int -> Posting ()
    p i = Posting (DocId i) ()
