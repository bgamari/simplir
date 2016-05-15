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
import qualified Data.Text.Internal as T.I
import qualified Data.Text.Unsafe as T.Unsafe
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as VU
import Types
import Term

tokenise :: T.Text -> [Term]
tokenise = map Term.fromText . T.words . T.toCaseFold

tokeniseWithPositions :: T.Text -> [(Term, Position)]
tokeniseWithPositions t@(T.I.Text _ _ len) = unfoldr f (0,0,0,0)
  where
    flushToken :: (Int, Int, Int, Int) -> Int -> Maybe ((Term, Position), (Int, Int, Int, Int))
    flushToken (!off, !tokN, !startChar, !curChar) off'
      | startChar == curChar = f (off+1, tokN, startChar+1, curChar+1)
      | otherwise =
        let !tok = Term.fromText $ T.Unsafe.takeWord16 tokLen $ T.Unsafe.dropWord16 startChar t
            !tokLen = curChar - startChar
            !pos = Position { charOffset  = Span startChar (curChar-1)
                            , tokenN = tokN
                            }
            !s'  = (off', tokN+1, curChar+1, curChar+1)
        in Just ((tok, pos), s')

    f :: (Int, Int, Int, Int) -> Maybe ((Term, Position), (Int, Int, Int, Int))
    f (off, _tokN, _startChar, !curChar)
        -- Done, nothing left over
      | off > len
      = Nothing

    f (!off, !tokN, !startChar, !curChar)
        -- end of stream
      | off == len
      = flushToken (off, tokN, startChar, curChar) off'

        -- new token
      | curChar < len && isSpace c
      = flushToken (off, tokN, startChar, curChar) off'

        -- within a token
      | otherwise
      = f (off', tokN, startChar, curChar+1)

      where
        T.Unsafe.Iter !c !delta = T.Unsafe.iter t off
        !off' = off + delta

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
