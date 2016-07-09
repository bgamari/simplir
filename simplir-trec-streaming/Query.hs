{-# LANGUAGE GADTs #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Query
    ( -- * Sets of queries
      Queries(..)
    , QueryId(..)
      -- * Misc
    , RecordedValueName(..)
    , QueryNodeName(..)
    , FieldName(..)
    , RetrievalModel(..)
    , WikiId(..)
      -- * Query tree
    , QueryNode(..)
    , collectFieldTerms
    ) where

import Control.Monad (guard)
import Control.Applicative
import Data.Foldable (toList)
import Data.Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Type.Equality
import qualified Data.Map as M
import Data.Text (Text)

import Numeric.Log
import SimplIR.Term as Term
import SimplIR.RetrievalModels.QueryLikelihood as QL
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac
import Parametric

newtype QueryId = QueryId Text
                deriving (Show, Eq, Ord, ToJSON, FromJSON)

newtype WikiId = WikiId Text
               deriving (Show, Eq, Ord, ToJSON, FromJSON)

deriving instance ToJSON Score
deriving instance FromJSON Score

newtype Queries = Queries { getQueries :: M.Map QueryId QueryNode }

instance FromJSON Queries where
    parseJSON = withArray "queries" $ fmap (Queries . M.fromList . toList) . traverse query
      where
        query = withObject "query" $ \o ->
          (,) <$> o .: "name" <*> o .: "query"

newtype RecordedValueName = RecordedValueName Text
                          deriving (Show, Eq, Ord, ToJSON, FromJSON)

newtype QueryNodeName = QueryNodeName Text
                      deriving (Show, Eq, Ord, ToJSON, FromJSON)

data FieldName a where
    FieldFreebaseIds :: FieldName Fac.EntityId
    FieldText        :: FieldName Term

eqFieldName :: FieldName a -> FieldName b -> Maybe (a :~: b)
eqFieldName FieldFreebaseIds FieldFreebaseIds = Just Refl
eqFieldName FieldText FieldText = Just Refl
eqFieldName _ _ = Nothing

deriving instance Show a => Show (FieldName a)
deriving instance Eq (FieldName a)

data RetrievalModel term
    = QueryLikelihood (Parametric (QL.Distribution term -> QL.Smoothing term))

data QueryNode = SumNode { name     :: Maybe QueryNodeName
                         , children :: [QueryNode]
                         }
               | ProductNode { name     :: Maybe QueryNodeName
                             , children :: [QueryNode]
                             }
               | ScaleNode { name   :: Maybe QueryNodeName
                           , scalar :: Parametric Double
                           , child  :: QueryNode
                           }
               | forall term.
                 RetrievalNode { name           :: Maybe QueryNodeName
                               , retrievalModel :: RetrievalModel term
                               , field          :: FieldName term
                               , terms          :: [term]
                               }

instance FromJSON QueryNode where
    parseJSON = withObject "query node" $ \o ->
      let nodeName = fmap QueryNodeName <$> o .:? "name"

          hasType :: String -> Aeson.Parser ()
          hasType ty = o .: "type" >>= guard . (ty ==)

          isAggregator :: String -> Aeson.Parser ()
          isAggregator operator = do
              hasType "aggregator"
              op <- o .: "op"
              guard (op == operator)

          sumNode = SumNode
              <$> nodeName
              <*  isAggregator "sum"
              <*> o .: "children"
          productNode = ProductNode
              <$> nodeName
              <*  isAggregator "product"
              <*> o .: "children"
          scaleNode = ScaleNode
              <$> nodeName
              <*  hasType "weight"
              <*> o .: "weight"
              <*> o .: "child"
          retrievalNode = do
              hasType "score"
              modelObj <- o .: "retrieval_model"
              fieldName <- o .: "field"
              case fieldName :: String of
                  "freebase_id" -> do
                      model <- parseModel modelObj
                      RetrievalNode <$> nodeName
                                    <*> pure model
                                    <*> pure FieldFreebaseIds
                                    <*> modelObj .: "terms"
                  "text"        -> do
                      model <- parseModel modelObj
                      RetrievalNode <$> nodeName
                                    <*> pure model
                                    <*> pure FieldText
                                    <*> modelObj .: "terms"
                  _             ->
                      fail $ "Unknown field name "++fieldName
      in sumNode <|> productNode <|> scaleNode <|> retrievalNode

collectFieldTerms :: FieldName term -> QueryNode -> [term]
collectFieldTerms f SumNode {..}       = foldMap (collectFieldTerms f) children
collectFieldTerms f ProductNode {..}   = foldMap (collectFieldTerms f) children
collectFieldTerms f ScaleNode {..}     = collectFieldTerms f child
collectFieldTerms f RetrievalNode {..}
  | Just Refl <- field `eqFieldName` f = terms
  | otherwise                          = []

parseModel :: Object -> Aeson.Parser (RetrievalModel term)
parseModel o = do
    modelType <- o .: "type"
    case modelType of
      "ql" -> do
          s <- o .: "smoothing"
          smoothingType <- s .: "type"
          QueryLikelihood <$> case smoothingType :: String of
              "dirichlet" -> pure . Dirichlet <$> o .: "mu"
              "jm"        -> do
                  fg <- o .: "alpha_foreground"
                  bg <- o .: "alpha_background"
                  let alpha = fg / (fg + bg) + undefined -- FIXME
                  pure . JelinekMercer <$> pure alpha
              _           -> fail $ "Unknown smoothing method "++smoothingType
      _  -> fail $ "Unknown retrieval model "++modelType


instance ToJSON QueryNode where
    toJSON (SumNode {..}) = object
        $ withName name
        [ "type"     .= str "aggregator"
        , "op"       .= str "sum"
        , "children" .= children
        ]
    toJSON (ProductNode {..}) = object
        $ withName name
        [ "type"     .= str "aggregator"
        , "op"       .= str "product"
        , "name"     .= name
        , "children" .= children
        ]
    toJSON (ScaleNode {..}) = object
        $ withName name
        [ "type"     .= str "weight"
        , "name"     .= name
        , "child"    .= child
        ]
    toJSON (RetrievalNode {..}) = object
        $ withName name
        []

str :: String -> String
str = id

withName :: Maybe QueryNodeName -> [Aeson.Pair] -> [Aeson.Pair]
withName (Just name) = (("name" .= name) :)
withName _           = id
