{-# LANGUAGE BangPatterns #-}

module Tokenise where

import Data.List (unfoldr)
import Data.Char (isSpace)

import qualified Data.Text as T
import Types

type TermPostings a = [(Term, Posting a)]

toPostings :: DocumentId -> [(Term, a)] -> TermPostings a
toPostings docId terms =
    [ (term, Posting docId pos)
    | (term, pos) <- terms
    ]

tokenise :: T.Text -> [Term]
tokenise = map Term . T.words . T.toCaseFold

tokeniseWithPos :: T.Text -> [(Term, Position)]
tokeniseWithPos t = unfoldr f (0,0,0)
  where
    f :: (Int, Int, Int) -> Maybe ((Term, Position), (Int, Int, Int))
    f (!tokN, !startChar, !off)
      | off == T.length t
      = Nothing

      | isSpace c
      , startChar == off
      = f (tokN, startChar+1, off+1)

      | isSpace c
      = let tok = Term $ T.take len $ T.drop startChar t
            len = off - startChar
            pos = Position { charOffset  = Span startChar (off-1)
                           , tokenN = tokN
                           }
            s'  = (tokN+1, off+1, off+1)
        in Just ((tok, pos), s')

      | otherwise
      = f (tokN, startChar, off+1)

      where
        c = t `T.index` off
