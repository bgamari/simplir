{-# LANGUAGE BangPatterns #-}

module Tokenise where

import Data.List (unfoldr)
import Data.Foldable
import qualified Data.DList as DList
import Data.Monoid
import Data.Profunctor
import Data.Foldable (foldl')
import Data.Char (isSpace)
import qualified Control.Foldl as Foldl
import Control.Foldl (Fold(..))

import qualified Data.Text as T
import qualified Data.Map.Strict as M
import qualified Data.Sequence as Seq
import qualified Data.Vector.Unboxed as VU
import Types

type TermPostings a = [(Term, Posting a)]

toPostings :: DocumentId -> [(Term, a)] -> TermPostings a
toPostings docId terms =
    [ (term, Posting docId pos)
    | (term, pos) <- terms
    ]

tokenise :: T.Text -> [Term]
tokenise = map Term . T.words . T.toCaseFold

tokeniseWithPositions :: T.Text -> [(Term, Position)]
tokeniseWithPositions t = unfoldr f (0,0,0)
  where
    f :: (Int, Int, Int) -> Maybe ((Term, Position), (Int, Int, Int))
    f (!tokN, !startChar, !off)
        -- empty token
      | off < T.length t
      , isSpace c
      , startChar == off
      = f (tokN, startChar+1, off+1)

        -- new token
      | off == T.length t || (off < T.length t && isSpace c)
      = let tok = Term $ T.take len $ T.drop startChar t
            len = off - startChar
            pos = Position { charOffset  = Span startChar (off-1)
                           , tokenN = tokN
                           }
            s'  = (tokN+1, off+1, off+1)
        in Just ((tok, pos), s')

        -- Done, nothing left over
      | off >= T.length t
      = Nothing

        -- in a token
      | otherwise
      = f (tokN, startChar, off+1)

      where
        c = t `T.index` off

foldTokens :: Fold a b -> [(Term, a)] -> M.Map Term b
foldTokens (Fold step initial extract) =
    fmap extract
    . foldl' (\acc (term, x) -> M.alter (f x) term acc) M.empty
  where
    f x Nothing   = Just $ step initial x
    f x (Just !x0) = Just $ step x0 x

data Pair a b = Pair !a !b

instance (Monoid a, Monoid b) => Monoid (Pair a b) where
    mempty = Pair mempty mempty
    Pair a b `mappend` Pair x y = Pair (a <> x) (b <> y)

accumPositions :: Fold Position (VU.Vector Position)
accumPositions =
    dimap (\pos -> Pair (Sum 1) (DList.singleton pos))
          (\(Pair (Sum n) xs) -> VU.fromListN n $ toList xs)
    Foldl.mconcat
