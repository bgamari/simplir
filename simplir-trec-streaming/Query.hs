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
    , TokenOrPhrase(..)
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
import Data.Foldable (fold, toList)
import Data.Aeson
import qualified Data.Vector as V
import qualified Data.Aeson.Types as Aeson
import Data.Type.Equality
import qualified Data.Map as M
import Data.Text (Text)

import Numeric.Log
import SimplIR.Types (TokenOrPhrase(..))
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
    parseJSON = withArray "queries" $ fmap (Queries . fold) . traverse query
      where
        query = withObject "query" $ \o ->
          M.singleton <$> o .: "name" <*> o .: "child"

newtype RecordedValueName = RecordedValueName Text
                          deriving (Show, Eq, Ord, ToJSON, FromJSON)

newtype QueryNodeName = QueryNodeName Text
                      deriving (Show, Eq, Ord, ToJSON, FromJSON)

data FieldName a where
    FieldFreebaseIds :: FieldName Fac.EntityId
    FieldText        :: FieldName (TokenOrPhrase Term)

eqFieldName :: FieldName a -> FieldName b -> Maybe (a :~: b)
eqFieldName FieldFreebaseIds FieldFreebaseIds = Just Refl
eqFieldName FieldText FieldText = Just Refl
eqFieldName _ _ = Nothing

deriving instance Show a => Show (FieldName a)
deriving instance Eq (FieldName a)

data RetrievalModel term
    = QueryLikelihood (Parametric (QL.Distribution term -> QL.Smoothing term))

data QueryNode = ConstNode { value :: Parametric Double }
               | SumNode { name         :: Maybe QueryNodeName
                         , children     :: [QueryNode]
                         , recordOutput :: Maybe RecordedValueName
                         }
               | ProductNode { name         :: Maybe QueryNodeName
                             , children     :: [QueryNode]
                             , recordOutput :: Maybe RecordedValueName
                             }
               | ScaleNode { name         :: Maybe QueryNodeName
                           , scalar       :: Parametric Double
                           , child        :: QueryNode
                           , recordOutput :: Maybe RecordedValueName
                           }
               | forall term.
                 RetrievalNode { name           :: Maybe QueryNodeName
                               , retrievalModel :: RetrievalModel term
                               , field          :: FieldName term
                               , terms          :: V.Vector (term, Double)
                               , recordOutput   :: Maybe RecordedValueName
                               }

instance FromJSON QueryNode where
    parseJSON = withObject "query node" $ \o ->
      let nodeName = fmap QueryNodeName <$> o .:? "name"
          weightedTerm val = weighted val <|> unweighted val
            where
              unweighted val = (\x -> (x, 1)) <$> parseJSON val
              weighted = withObject "weighted term" $ \t ->
                (,) <$> t .: "term" <*> t .: "weight"

          record :: Aeson.Parser (Maybe RecordedValueName)
          record = do
              v <- o .:? "record" .!= Bool False
              case v of
                Bool True   -> Just <$> o .: "name"
                Bool False  -> return Nothing
                String name -> return $ Just $ RecordedValueName name
                _           -> fail $ "unknown value for 'record': "++show v

          constNode = ConstNode <$> o .: "value"

          aggregatorNode = do
              op <- o .: "op"
              case op :: String of
                "product" -> ProductNode <$> nodeName <*> o .: "children" <*> record
                "sum"     -> SumNode <$> nodeName <*> o .: "children" <*> record
                _         -> fail "Unknown aggregator node type"

          scaleNode = ScaleNode
              <$> nodeName
              <*> o .: "scalar"
              <*> o .: "child"
              <*> record

          retrievalNode = do
              modelObj <- o .: "retrieval_model"
              fieldName <- o .: "field"
              let terms :: FromJSON term => Aeson.Parser (V.Vector (term, Double))
                  terms = mapM weightedTerm =<< o .: "terms"
              case fieldName :: String of
                  "freebase_id" -> do
                      model <- parseModel modelObj
                      RetrievalNode <$> nodeName
                                    <*> pure model
                                    <*> pure FieldFreebaseIds
                                    <*> terms
                                    <*> record
                  "text"        -> do
                      model <- parseModel modelObj
                      RetrievalNode <$> nodeName
                                    <*> pure model
                                    <*> pure FieldText
                                    <*> terms
                                    <*> record
                  _             -> fail $ "Unknown field name "++fieldName
      in do ty <- o .: "type"
            case ty :: String of
              "aggregator" -> aggregatorNode
              "constant" -> constNode
              "scale" -> scaleNode
              "scoring_model" -> retrievalNode
              _ -> fail "Unknown node type"

collectFieldTerms :: FieldName term -> QueryNode -> [term]
collectFieldTerms _ ConstNode {..}     = []
collectFieldTerms f SumNode {..}       = foldMap (collectFieldTerms f) children
collectFieldTerms f ProductNode {..}   = foldMap (collectFieldTerms f) children
collectFieldTerms f ScaleNode {..}     = collectFieldTerms f child
collectFieldTerms f RetrievalNode {..}
  | Just Refl <- field `eqFieldName` f = map fst $ toList terms
  | otherwise                          = []

parseModel :: Object -> Aeson.Parser (RetrievalModel term)
parseModel o = do
    modelType <- o .: "type"
    case modelType of
      "ql" -> do
          s <- o .: "smoothing"
          smoothingType <-  s .: "type"
          QueryLikelihood <$> case smoothingType :: String of
              "dirichlet" -> pure . Dirichlet <$> s .: "mu"
              "jm"        -> do
                  fgP <- s .: "alpha_foreground" :: Aeson.Parser (Parametric Double)
                  bgP <- s .: "alpha_background" :: Aeson.Parser (Parametric Double)
                  let alpha :: Parametric (Log Double)
                      alpha = (\fg bg -> realToFrac $ fg / (fg + bg)) <$> fgP <*> bgP
                  pure (JelinekMercer <$> alpha)
              _           -> fail $ "Unknown smoothing method "++smoothingType
      _  -> fail $ "Unknown retrieval model "++modelType


instance ToJSON QueryNode where
    toJSON (ConstNode {..}) = object
        [ "type"     .= str "constant"
        , "value"    .= value
        ]
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
