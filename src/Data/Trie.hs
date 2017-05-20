{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Trie where

import Data.Hashable
import Data.Maybe
import Control.Applicative ((<|>))
import Data.Foldable
import Data.Monoid
import qualified Data.HashMap.Strict as HM
import Prelude hiding (lookup)

data Trie a r = Trie (Maybe r) (HM.HashMap a (Trie a r))
              deriving (Show, Functor, Foldable)

instance (Eq a, Hashable a) => Monoid (Trie a r) where
    mempty = empty
    Trie ra ma `mappend` Trie rb mb =
        Trie (ra <|> rb) (HM.unionWith mappend ma mb)

empty :: (Eq a, Hashable a) => Trie a r
empty = Trie Nothing mempty

singleton :: (Eq a, Hashable a) => [a] -> r -> Trie a r
singleton xs r = insert xs r empty

fromList :: (Eq a, Hashable a) => [([a], r)] -> Trie a r
fromList = foldl' (\trie (xs, r) -> insert xs r trie) empty

toList :: Trie a r -> [([a], r)]
toList = go []
  where
    go acc (Trie mr m) =
        first $ foldMap (\(k,v) -> go (k:acc) v) (HM.toList m)
      where
        first = case mr of Just r  -> ((reverse acc, r) :)
                           Nothing -> id

insert :: (Eq a, Hashable a) => [a] -> r -> Trie a r -> Trie a r
insert xs0 r0 = go xs0
  where
    go [] (Trie _ m) = Trie (Just r0) m
    go (x:xs) (Trie r m) =
        Trie r (HM.alter f x m)
      where
        f Nothing   = Just $ singleton xs r0
        f (Just m0) = Just $ go xs m0

lookup :: (Eq a, Hashable a) => [a] -> Trie a r -> Maybe r
lookup = flip go
  where
    go (Trie r _) [] = r
    go trie (x:xs) =
        case step x trie of
          Just trie' -> go trie' xs
          Nothing    -> Nothing

step :: (Eq a, Hashable a) => a -> Trie a r -> Maybe (Trie a r)
step x (Trie _ m) = HM.lookup x m

matches :: forall a r. (Eq a, Hashable a)
        => [a] -> Trie a r -> [([a], r)]
matches = matches' id

matches' :: forall a b r. (Eq a, Hashable a)
         => (b -> a) -> [b] -> Trie a r -> [([b], r)]
matches' extract xs0 trie0 = go xs0 []
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

