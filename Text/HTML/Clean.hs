{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module HtmlClean (clean, HtmlDocument(..)) where

import Data.Maybe (listToMaybe, fromMaybe, mapMaybe)
import Data.List (unfoldr)
import Control.Applicative

import Control.DeepSeq
import qualified Data.HashSet as HS
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Data.Text (Text)
import Text.HTML.TagSoup

-- | Take the children of the first occurrence of the given tag.
insideTag :: Text -> [Tag Text] -> [Tag Text]
insideTag name =
      takeWhile (not . isTagCloseName name)
    . dropWhile (not . isTagOpenName name)

-- | Drop all instances of the given tag.
dropTag :: Text -> [Tag Text] -> [Tag Text]
dropTag name = taking
  where
    taking :: [Tag Text] -> [Tag Text]
    taking []                  = []
    taking (x:xs)
      | isTagOpenName name x   = dropping xs
      | otherwise              = x : taking xs

    dropping []                = []
    dropping (x:xs)
      | isTagCloseName name x  = taking xs
    dropping (_:xs)            = dropping xs

extractTitle :: [Tag Text] -> TL.Text
extractTitle =
    innerText' . insideTag "title"

-- | Take the inner text of the first occurrence of the given tag
tagInnerText :: Text -> [Tag Text] -> Maybe TL.Text
tagInnerText name tags =
    case insideTag name tags of
        []       -> Nothing
        children -> Just $ innerText' children

extractBody :: [Tag Text] -> TL.Text
extractBody tags =
    let tags' = foldr (dropTag) tags
                [ "style" , "nav" , "video"
                , "canvas" , "script" ]
    in fromMaybe ""
       $  tagInnerText "article" tags'
      <|> tagInnerText "main" tags'
      <|> tagInnerText "body" tags'

innerText' :: [Tag Text] -> TL.Text
innerText' = TL.fromChunks . map takeText
  where
    takeText (TagText t)                = t
    takeText (TagClose tag)
      | tag `HS.member` needsWhitespace = " "
    takeText (TagOpen tag _)
      | tag `HS.member` needsWhitespace = " "
    takeText _                          = ""

-- | Elements whose opening and closing tags should be mapped to whitespace in
-- inner-text.
needsWhitespace :: HS.HashSet Text
needsWhitespace = HS.fromList
    [ -- Block-level elements
      "address"
    , "article"
    , "aside"
    , "blockquote"
    , "canvas"
    , "dd"
    , "div"
    , "dl"
    , "fieldset"
    , "figcaption"
    , "figure"
    , "footer"
    , "form"
    , "h1", "h2", "h3", "h4", "h5", "h6"
    , "header"
    , "hgroup"
    , "hr"
    , "li"
    , "main"
    , "nav"
    , "noscript"
    , "ol"
    , "output"
    , "p"
    , "pre"
    , "section"
    , "table"
    , "tfoot"
    , "ul"
    , "video"

      -- others
    , "tr", "td", "th"
    , "br"
    ]

-- | Extract the essence of an HTML document.
clean :: Text -> HtmlDocument
clean content =
    let tags = canonicalizeTags
             $ parseTagsOptions parseOptionsFast content
        docTitle = extractTitle tags
        docBody = extractBody tags
    in HtmlDocument {..}

data HtmlDocument = HtmlDocument { docTitle :: !TL.Text
                                 , docBody  :: !TL.Text
                                 }
                  deriving (Show)

instance NFData HtmlDocument where
    rnf (HtmlDocument {..}) =
        TL.length docTitle `seq` TL.length docBody `seq` ()
