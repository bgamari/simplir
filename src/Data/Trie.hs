{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Trie
    ( Trie
      -- * Construction
    , empty
    , singleton
    , insert
    , fromList
      -- * Queries
    , lookup
    , toList
      -- * Sequential matches
    , sequentialMatch
    , sequentialMatches
      -- * Overlapping matches
    , overlappingMatches
    , overlappingMatches'
    ) where

import Data.Hashable
import Data.Maybe
import Data.Foldable hiding (toList)
import Data.Semigroup
import qualified Data.HashMap.Strict as HM
import Prelude hiding (lookup)

-- | A trie with characters @a@ and terminal values @r@.
data Trie a r = Trie (Maybe r) (HM.HashMap a (Trie a r))
              deriving (Show, Functor, Foldable)

instance (Eq a, Hashable a, Semigroup r) => Monoid (Trie a r) where
    mempty = empty
    mappend = (<>)

instance (Eq a, Hashable a, Semigroup r) => Semigroup (Trie a r) where
    Trie ra ma <> Trie rb mb =
        Trie (ra <> rb) (HM.unionWith mappend ma mb)

-- | An empty trie.
empty :: (Eq a, Hashable a) => Trie a r
empty = Trie Nothing mempty

-- | A trie consisting of a single terminating sequence.
singleton :: (Eq a, Hashable a) => [a] -> r -> Trie a r
singleton xs r = insert xs r empty

-- | A trie built from a list of terminating sequences.
fromList :: (Eq a, Hashable a) => [([a], r)] -> Trie a r
fromList = foldl' (\trie (xs, r) -> insert xs r trie) empty

-- | List the terminating sequences of a trie.
toList :: Trie a r -> [([a], r)]
toList = go []
  where
    go acc (Trie mr m) =
        first $ foldMap (\(k,v) -> go (k:acc) v) (HM.toList m)
      where
        first = case mr of Just r  -> ((reverse acc, r) :)
                           Nothing -> id

-- | Insert a sequence into a trie.
insert :: (Eq a, Hashable a) => [a] -> r -> Trie a r -> Trie a r
insert xs0 r0 = go xs0
  where
    go [] (Trie _ m) = Trie (Just r0) m
    go (x:xs) (Trie r m) =
        Trie r (HM.alter f x m)
      where
        f Nothing   = Just $ singleton xs r0
        f (Just m0) = Just $ go xs m0

-- | Lookup the terminal value of a sequence.
lookup :: (Eq a, Hashable a) => [a] -> Trie a r -> Maybe r
lookup = flip go
  where
    go (Trie r _) [] = r
    go trie (x:xs) =
        case step x trie of
          Just trie' -> go trie' xs
          Nothing    -> Nothing
{-# INLINE lookup #-}

step :: (Eq a, Hashable a) => a -> Trie a r -> Maybe (Trie a r)
step x (Trie _ m) = HM.lookup x m
{-# INLINE step #-}

data Matches c a = Match !a   (Matches c a) -- ^ A match was found
                 | NoMatch !c (Matches c a) -- ^ No match was found; returns the non-matching character.
                 | EndOfSequence            -- ^ End of sequence
                 deriving (Functor)

-- | Find the maximal match of the given sequence, returning the terminal value
-- and the suffix.
sequentialMatch :: (Eq c, Hashable c)
                => Trie c a -> [c] -> Maybe (a, [c])
sequentialMatch = go
  where
    go trie (c:cs)
      | Just next <- step c trie
      = go next cs
    go (Trie (Just terminal) _) cs
      = Just (terminal, cs)
    go _ _
      = Nothing

-- | Given a sequence, find the sequence of non-overlapping maximal matches of
-- the given trie.
sequentialMatches :: forall a r. (Eq a, Hashable a)
                  => [a] -> Trie a r -> Matches a r
sequentialMatches xs0 trie = go xs0
  where
    go cs
      | Just (x, rest) <- sequentialMatch trie cs
      = Match x (go rest)
    go (c:cs) = NoMatch c (go cs)
    go [] = EndOfSequence

-- | Given a sequence, find the sequence of potentially overlapping maximal
-- matches of the given trie.
overlappingMatches :: forall a r. (Eq a, Hashable a)
                   => [a] -> Trie a r -> [([a], r)]
overlappingMatches = overlappingMatches' id
{-# INLINE overlappingMatches #-}

-- | Given a sequence, find the sequence of maximal matches of the given trie
-- along with the sequence values.
overlappingMatches' :: forall a b r. (Eq a, Hashable a)
                    => (b -> a) -> [b] -> Trie a r -> [([b], r)]
overlappingMatches' extract xs0 trie0 = go xs0 []
  where
    go :: [b] -> [([b], Trie a r)] -> [([b], r)]
    go []     _tries = []
    go (x:xs) tries  =
        let tries' = mapMaybe stepTrie (([], trie0) : tries)

            stepTrie :: ([b], Trie a r) -> Maybe ([b], Trie a r)
            stepTrie (history, trie) =
                case step (extract x) trie of
                  Just trie' -> Just (x:history, trie')
                  Nothing    -> Nothing
        in mapMaybe emitMatch tries' ++ go xs tries'

    emitMatch :: ([b], Trie a r) -> Maybe ([b], r)
    emitMatch (history, Trie (Just x) _) = Just (reverse history, x)
    emitMatch _ = Nothing
{-# INLINE overlappingMatches' #-}
