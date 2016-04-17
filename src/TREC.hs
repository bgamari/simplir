{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module TREC where

import Data.Maybe (mapMaybe)

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.CaseInsensitive as CI

import Text.HTML.Parser

data Document = Document { docNo       :: Text
                         , docDate     :: Text
                         , docHeadline :: Text
                         , docText     :: Text
                         }
              deriving (Show)

trecDocuments :: TL.Text -> [Document]
trecDocuments = go . parseTokensLazy
  where
    go :: [Token] -> [Document]
    go [] = []
    go xs =
      let (tags, rest) = span (not . isCloseTag "DOC")
                       $ dropWhile (not . isOpenTag "DOC") xs

          tagContent name = takeContent $ takeInsideTag name tags
          docNo = tagContent "DOCNO"
          docDate = tagContent "DATE"
          docHeadline = tagContent "HEADLINE"
          docText = tagContent "TEXT"
      in if T.null docNo
            then go rest
            else Document {..} : go rest

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
