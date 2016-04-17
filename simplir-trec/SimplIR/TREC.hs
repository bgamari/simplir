{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module SimplIR.TREC where

import Control.Monad (unless)
import Control.Monad.Trans.Class
import Data.Maybe (mapMaybe)

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.CaseInsensitive as CI
import Pipes
import qualified Pipes.Prelude as P.P
import Pipes.Attoparsec as P.A
import qualified Pipes.Parse as Parse
import Control.Lens

import Text.HTML.Parser

data Document = Document { docNo       :: Text
                         , docDate     :: Text
                         , docHeadline :: Text
                         , docText     :: Text
                         }
              deriving (Show)

trecDocuments' :: Monad m => Producer T.Text m a -> Producer Document m ()
trecDocuments' = go . P.A.parsed token
  where
    go :: Monad m => Producer Token m a -> Producer Document m ()
    go xs = do
        prefix <- xs ^. Parse.span (not . isOpenTag "DOC") >-> P.P.drain
        (toks, rest) <- lift $ P.P.toListM' $ prefix ^. Parse.span (not . isCloseTag "DOC")
        unless (null toks) $ do yield $ tokensToDocument toks
                                go rest

trecDocuments :: TL.Text -> [Document]
trecDocuments = go . parseTokensLazy
  where
    go :: [Token] -> [Document]
    go [] = []
    go xs =
      let (tags, rest) = span (not . isCloseTag "DOC")
                       $ dropWhile (not . isOpenTag "DOC") xs
      in tokensToDocument tags : go rest

tokensToDocument :: [Token] -> Document
tokensToDocument toks =
  let tagContent name = takeContent $ takeInsideTag name toks
      docNo = tagContent "DOCNO"
      docDate = tagContent "DATE"
      docHeadline = tagContent "HEADLINE"
      docText = tagContent "TEXT"
  in Document {..}

takeContent :: [Token] -> Text
takeContent = T.concat . mapMaybe getContent
  where
    getContent (ContentText t) = Just t
    getContent (ContentChar c) = Just $ T.singleton c
    getContent _               = Nothing

takeInsideTag :: TagName -> [Token] -> [Token]
takeInsideTag name =
      takeWhile (not . isCloseTag name)
    . dropWhile (not . isOpenTag name)

isOpenTag :: TagName -> Token -> Bool
isOpenTag name (TagOpen name' _) = CI.mk name == CI.mk name'
isOpenTag _    _                 = False

isCloseTag :: TagName -> Token -> Bool
isCloseTag name (TagClose name') = CI.mk name == CI.mk name'
isCloseTag _    _                = False
