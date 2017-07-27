{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}

module SimplIR.DiskIndex.Posting.Collect
    ( collectPostings
      -- * Tests
    , test
    , tests
    ) where

import Data.Monoid
import Data.Function (on)

import qualified Data.Heap as H

import Pipes
import SimplIR.Types
import SimplIR.Term

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Pipes.Prelude as P.P
import Test.Tasty
import Test.Tasty.QuickCheck

data ActivePosting m p = AP { apPosting   :: !(Posting p)
                            , apTerm      :: !Term
                            , apRemaining :: Producer (Posting p) m ()
                            }

apDocId :: ActivePosting m p -> DocumentId
apDocId = postingDocId . apPosting

instance Eq (ActivePosting m p) where
    (==) = (==) `on` (postingDocId . apPosting)

instance Ord (ActivePosting m p) where
    compare = compare `on` (postingDocId . apPosting)

-- | Given a set of terms and their sorted postings, collect the postings for
-- all documents.
--
-- >>> let p docId :: Int -> Posting ()
-- >>>     p i = Posting (DocId i) ()
-- >>>
-- >>> Pipes.Prelude.toList $ collectPostings
-- >>>      [ ("cat", each $ map p [1, 3])
-- >>>      , ("dog", each $ map p [1, 2, 4])
-- >>>      , ("rat", each $ map p [2, 3])
-- >>>      ]
-- [ (DocId 1, [("cat", ()), ("dog", ())])
-- , (DocId 2, [("rat", ()), ("dog", ())])
-- , (DocId 3, [("cat", ()), ("dog", ())])
-- , (DocId 4, [("dog", ())])
-- ]
collectPostings :: forall m p. (Monad m)
                => [(Term, Producer (Posting p) m ())]
                -> Producer (DocumentId, [(Term, p)]) m ()
collectPostings = start
  where
    start prods = do
        -- request one element from each producer
        s0 <- lift $ mapM nextPosting prods
        go (mconcat s0)

    -- | Request a posting
    nextPosting :: (Term, Producer (Posting p) m ())
                -> m (H.Heap (ActivePosting m p))
    nextPosting (term, prod) = do
        mx <- next prod
        case mx of
            Right (posting, actives) -> return $ H.singleton $ AP posting term actives
            Left ()                  -> return H.empty

    go :: H.Heap (ActivePosting m p)
       -> Producer (DocumentId, [(Term, p)]) m ()
    go actives
      | H.null actives     = return ()
      | otherwise          = do
        -- pop off minimal active postings
        let minDocId = apDocId $ H.minimum actives
            (postings, actives') = H.span (\x -> apDocId x == minDocId) actives

            docPostings :: [(Term, p)]
            docPostings =
                [ (apTerm, postingBody apPosting)
                | AP {..} <- H.toUnsortedList postings ]

        yield (minDocId, docPostings)

        -- pull in new postings
        new <- lift $ mapM (\ap -> nextPosting (apTerm ap, apRemaining ap))
                    $ H.toUnsortedList postings
        go (actives' <> mconcat new)

test :: Monad m => [(Term, Producer (Posting ()) m ())]
test =
    [ ("cat",    each [p 1, p 3, p 4, p 7])
    , ("turtle", each [p 2, p 3, p 7, p 9])
    , ("dog",    each [p 5, p 100, p 3000])
    ]
  where
    p :: Int -> Posting ()
    p i = Posting (DocId i) ()

roundTripPostings :: forall p. (Ord p, Show p)
                  => M.Map DocumentId (M.Map Term p)
                  -> Property
roundTripPostings docs =
    counterexample (show postings) $
    docs' === M.filter (not . M.null) docs
  where
    docs' :: M.Map DocumentId (M.Map Term p)
    docs' =
        M.fromList -- no need to append here; collection should handle this
        $ map (fmap M.fromList)
        $ P.P.toList
        $ collectPostings
        $ M.toAscList $ fmap (each . S.toAscList) postings

    postings :: M.Map Term (S.Set (Posting p))
    postings = M.unionsWith mappend
               [ M.singleton term (S.singleton $ Posting docId x)
               | (docId, terms) <- M.toList docs
               , (term, x) <- M.toList terms
               ]

tests :: TestTree
tests = testGroup "CollectPostings"
    [ testProperty "round-trip" (roundTripPostings @Int) ]
