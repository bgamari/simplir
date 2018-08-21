{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | A parser for the TREC Robust document corpus.
module SimplIR.TREC.Robust where

import Control.Applicative
import Control.Exception
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Pipes
import Pipes.Attoparsec as P.A
import Data.Attoparsec.Text as A
import SimplIR.TREC

data Document = Document { docNo       :: T.Text
                         , docHdr      :: Maybe T.Text
                         , docDate     :: Maybe T.Text
                         , docHeadline :: Maybe T.Text
                         , docBody     :: T.Text
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
    <$> {-# SCC "trecDocuments'" #-} P.A.parsed document prod

trecDocuments :: TL.Text -> [Document]
trecDocuments = parseMany document

document :: Parser Document
document = element "DOC" $ do
    Document
      <$> element "DOCNO" text
      <*> optional (element "DOCHDR" text)
      <*> optional (element "DATE" text)
      <*> optional (element "HEADLINE" text)
      <*  A.skipSpace
      <*> A.takeText
      <*  A.skipSpace
