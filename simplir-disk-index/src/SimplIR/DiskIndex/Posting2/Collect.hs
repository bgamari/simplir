{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module SimplIR.DiskIndex.Posting2.Collect where

import Data.Foldable
import Data.List.NonEmpty (NonEmpty(..))
import Data.Maybe
import Data.Function (on)

import qualified Data.Heap as H

import SimplIR.Types
import SimplIR.Term

import qualified Data.Map as M
import qualified Data.Set as S
import Test.Tasty
import Test.Tasty.QuickCheck

data ActivePosting term p = AP { apPosting   :: !(Posting p)
                               , apTerm      :: !term
                               , apRemaining :: [Posting p]
                               }

apDocId :: ActivePosting term p -> DocumentId
apDocId = postingDocId . apPosting
{-# INLINEABLE apDocId #-}

instance Eq (ActivePosting term p) where
    (==) = (==) `on` apDocId

instance Ord (ActivePosting term p) where
    compare = compare `on` apDocId

-- | Given a set of terms and their sorted postings, collect the postings for
-- all documents.
--
-- >>> let p docId :: Int -> Posting ()
-- >>>     p i = Posting (DocId i) ()
-- >>>
-- >>> collectPostings
-- >>>      [ ("cat", map p [1, 3])
-- >>>      , ("dog", map p [1, 2, 4])
-- >>>      , ("rat", map p [2, 3])
-- >>>      ]
-- [ (DocId 1, [("cat", ()), ("dog", ())])
-- , (DocId 2, [("rat", ()), ("dog", ())])
-- , (DocId 3, [("cat", ()), ("dog", ())])
-- , (DocId 4, [("dog", ())])
-- ]
collectPostings :: forall term p. ()
                => [(term, [Posting p])]
                -> [(DocumentId, [(term, p)])]
collectPostings = go . H.fromList . mapMaybe (uncurry initial)
  where
    initial :: term -> [Posting p] -> Maybe (ActivePosting term p)
    initial _ [] = Nothing
    initial t (p:ps) = Just $ AP p t ps

    go :: H.Heap (ActivePosting term p)
       -> [(DocumentId, [(term, p)])]
    go h
      | Just (aps@(ap :| _), h') <- takeMins h
      = let h'' = foldl' (flip H.insert) h'
                  [ ap''
                  | ap' <- toList aps
                  , Just ap'' <- pure $ initial (apTerm ap') (apRemaining ap')
                  ]
            termPostings = [ (apTerm ap', postingBody $ apPosting ap')
                           | ap' <- toList aps
                           ]
        in (apDocId ap, termPostings) : go h''
    go _ = []

takeMins :: Ord a => H.Heap a -> Maybe (NonEmpty a, H.Heap a)
takeMins h
  | Just (x,xs) <- H.uncons h
  = let (a,b) = H.span (==x) xs
    in Just (x :| toList a, b)
  | otherwise = Nothing

test :: [(Term, [Posting ()])]
test =
    [ ("cat",    [p 1, p 3, p 4, p 7])
    , ("turtle", [p 2, p 3, p 7, p 9])
    , ("dog",    [p 5, p 100, p 3000])
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
        $ collectPostings
        $ M.toAscList $ fmap S.toAscList postings

    postings :: M.Map Term (S.Set (Posting p))
    postings = M.unionsWith mappend
               [ M.singleton term (S.singleton $ Posting docId x)
               | (docId, terms) <- M.toList docs
               , (term, x) <- M.toList terms
               ]

tests :: TestTree
tests = testGroup "CollectPostings"
    [ testProperty "round-trip" (roundTripPostings @Int) ]
