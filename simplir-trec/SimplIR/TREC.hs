{-# LANGUAGE OverloadedStrings #-}

-- | Utilities for parsing TREC XML-style input.
module SimplIR.TREC
    ( -- * Primitives
      element
    , text
      -- * Parser type
    , A.Parser
    , parseMany
      -- * Errors
    , ParseError(..)
    ) where

import Control.Exception
import Control.Monad

import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Attoparsec.Text as A
import qualified Data.Attoparsec.Text.Lazy as AL

-- | Parse a TREC XML-style element, using the given parser to parse the body.
element :: T.Text      -- ^ tag name
        -> A.Parser a  -- ^ parser for body
        -> A.Parser a
element name parseBody = (A.<?> description) $ do
    A.skipSpace
    void $ A.string $ "<" <> name <> ">"
    A.skipSpace
    body <- scanUntil ("</" <> T.unpack name <> ">")
    A.skipSpace
    case A.parse parseBody $ T.dropEnd (3+T.length name) body of
      A.Fail i _ctxts err -> fail $ "Error parsing body of <" <> T.unpack name <> ">: " <> err <> ": " <> show i
      A.Done i r
        | T.null i -> return r
        | otherwise -> fail $ "Leftover input parsing body of <" <> T.unpack name <> ">: " <> show i
      A.Partial f ->
        case f T.empty of
          A.Fail i _ctxts err -> fail $ "Error parsing body of <" <> T.unpack name <> ">: " <> err <> ": " <> show i
          A.Done i r
            | T.null i -> return r
            | otherwise -> fail $ "Leftover input parsing body of <" <> T.unpack name <> ">: " <> show i
          A.Partial _ -> fail "Impossible"
  where
    description = T.unpack name <> " element"

scanUntil :: String -> A.Parser T.Text
scanUntil s = A.scan s f
  where
    f []     _    = Nothing
    f (x:xs) c
      | x == c    = Just xs
      | otherwise = Just s

-- | Take the body
text :: A.Parser T.Text
text = A.skipSpace *> A.takeText <* A.skipSpace

data ParseError = ParseError { parseErrorMsg :: String
                             , parseErrorRest :: TL.Text
                             }
instance Show ParseError where
    show x = parseErrorMsg x <> ": " <> TL.unpack (TL.take 200 $ parseErrorRest x)
instance Exception ParseError

-- | Lazily parse many items back-to-back. Throws 'ParseError' on failure.
parseMany :: A.Parser a -> TL.Text -> [a]
parseMany parse = go
  where
    go t
      | TL.null t = []
      | otherwise =
          case AL.parse parse t of
            AL.Fail rest _ctxts err -> throw $ ParseError err rest
            AL.Done rest x -> x : go rest
