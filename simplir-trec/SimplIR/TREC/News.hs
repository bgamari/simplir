{-# LANGUAGE OverloadedStrings #-}

-- | A parser for TREC News queries.
module SimplIR.TREC.News where

import Control.Applicative
import qualified Data.Text as T
import Data.Attoparsec.Text as A
import SimplIR.TREC

-- | A TREC News corpus topic.
data Topic = Topic { topicNumber    :: !Int
                   , topicDocNumber :: !T.Text
                   , topicUrl       :: !T.Text
                   , topicEntities  :: [Entity]
                   }
           deriving (Show)

data Entity = Entity { entityId :: T.Text
                     , entityMentions :: [T.Text]
                     , entityLink :: T.Text
                     }
            deriving (Show)

newsQuery :: A.Parser Topic
newsQuery = element "top" topic'
  where
    topic' :: A.Parser Topic
    topic' =
      Topic
        <$> element "num" ("Number: " *> A.decimal <* A.skipSpace)
        <*> element "docid" text
        <*> element "url" text
        <*> element "entities" (many entity)
    entity = element "entity" $ do
        Entity
            <$> element "id" text
            <*> many (element "mention" text)
            <*> element "link" text
