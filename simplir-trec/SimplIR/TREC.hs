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

-- | Parse a stream of 'Document's. May throw a 'ParsingError'.
trecDocuments' :: forall r m. (Monad m)
               => Producer T.Text m r -> Producer Document m r
trecDocuments' prod =
    either (error . show . fst) id <$> P.A.parsed document prod

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
        body <- scanUntil "</DOC>"
        A.skipSpace
        return $ doc { docBody = body }

    field s = do
      void $ A.string ("<" <> s <> ">")
      val <- scanUntil ("</" <> s <> ">")
      A.skipSpace
      return val

-- | Match the given string exactly.
scanUntil' :: T.Text -> A.Parser T.Text
scanUntil' s = A.scan 0 f
  where
    n = T.length s
    f i c
      | n == i = Nothing
      | c == (s `T.index` i) = Just (i+1)
      | otherwise      = Just 0

-- | Match the given string exactly.
scanUntil :: T.Text -> A.Parser T.Text
scanUntil s =
    T.dropEnd (T.length s) . fst <$> A.match go
  where
    go = A.string s <|> (A.anyChar *> go)

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
