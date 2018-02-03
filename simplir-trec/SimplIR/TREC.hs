{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.TREC where

import Control.Applicative
import Control.Exception
import Data.Monoid

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Pipes
import Pipes.Attoparsec as P.A
import qualified Data.Attoparsec.Text as A
import qualified Data.Attoparsec.Text.Lazy as AL

--import Text.HTML.Parser

data Document = Document { docNo       :: Text
                         , docHdr      :: Maybe Text
                         , docDate     :: Maybe Text
                         , docHeadline :: Maybe Text
                         , docBody     :: Text
                         }
              deriving (Show)

data TrecParseError = TrecParseError ParsingError
                    deriving (Show)

instance Exception TrecParseError

-- | Parse a stream of 'Document's. May throw a 'ParsingError'.
trecDocuments' :: forall r m. (Monad m)
               => Producer T.Text m r -> Producer Document m r
trecDocuments' prod =
    either (throw . TrecParseError . fst) id
    <$> {-# SCC "trecDocuments'" #-}P.A.parsed document prod

document :: A.Parser Document
document = do
    A.skipSpace
    "<DOC>" >> A.skipSpace
    docNo <- field "DOCNO"
    parseDoc $ Document { docNo = docNo
                        , docHdr = Nothing
                        , docDate = Nothing
                        , docHeadline = Nothing
                        , docBody = error "no body"
                        }
  where
    parseDoc doc =
            setDocHdr doc
        <|> setDocDate doc
        <|> setDocHeadline doc
        <|> setDocBody doc

    setDocHdr doc = do
        val <- field "DOCHDR"
        parseDoc $ doc { docHdr = Just val }

    setDocDate doc = do
        val <- field "DATE"
        parseDoc $ doc { docDate = Just val }

    setDocHeadline doc = do
        val <- field "HEADLINE"
        parseDoc $ doc { docHeadline = Just val }

    setDocBody doc = do
        body <- scanEndDoc
        A.skipSpace
        return $ doc { docBody = body }

    field s = do
      void $ A.string ("<" <> s <> ">")
      val <- scanUntil ("</" <> T.unpack s <> ">")
      A.skipSpace
      return val

-- | This is a very common case and we need to scan a lot of text. Consequently
-- it's worth a special case.
scanEndDoc :: A.Parser T.Text
scanEndDoc = A.scan 0 f
  where
    f :: Int -> Char -> Maybe Int
    f 0 '<' = Just 1
    f 1 '/' = Just 2
    f 2 'D' = Just 3
    f 3 'O' = Just 4
    f 4 'C' = Just 5
    f 5 '>' = Just 6
    f 6 _   = Nothing
    f _ _   = Just 0

scanUntil :: String -> A.Parser T.Text
scanUntil s = A.scan s f
  where
    f []     _    = Nothing
    f (x:xs) c
      | x == c    = Just xs
      | otherwise = Just s

data ParseError = ParseError { parseErrorMsg :: String
                             , parseErrorRest :: TL.Text
                             }
                deriving (Show)
instance Exception ParseError

trecDocuments :: TL.Text -> [Document]
trecDocuments = go
  where
    go :: TL.Text -> [Document]
    go t
      | TL.null t = []
      | otherwise =
          case AL.parse document t of
            AL.Fail rest _ctxts err -> throw $ ParseError err rest
            AL.Done rest doc -> doc : go rest
