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
      -- * Misc
    , kbaTokenise
    ) where

import Control.Applicative
import Data.Foldable (fold, toList)
import Data.Monoid
import Data.Aeson
import qualified Data.Vector as V
import qualified Data.Text as T
import qualified Data.HashSet as HS
import qualified Data.Map as M
import qualified Data.Aeson.Types as Aeson
import Data.Type.Equality
import Data.Text (Text)

import Numeric.Log
import SimplIR.Types (TokenOrPhrase(..), Position)
import SimplIR.Term as Term
import SimplIR.Tokenise
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
               | DropNode
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

               | CondNode { predicateTerms   :: V.Vector (TokenOrPhrase Term)
                          , predicateNegated :: Bool
                          , trueChild        :: QueryNode
                          , falseChild       :: QueryNode
                          }

instance FromJSON QueryNode where
    parseJSON = withObject "query node" $ \o ->
      let nodeName = fmap QueryNodeName <$> o .:? "name"

          weightedTerm :: FromJSON term => Aeson.Value -> Aeson.Parser (term, Double)
          weightedTerm val = weighted val <|> unweighted val
            where
              unweighted val = ((\x -> (x, 1)) <$> parseJSON val)
                            <|> Aeson.typeMismatch "term" val
              weighted = withObject "weighted term" $ \t ->
                (,) <$> t .: "term" <*> t .: "weight"

          -- TODO This seems a bit out of place
          splitTerms :: T.Text -> Maybe (TokenOrPhrase Term)
          splitTerms token =
              case map (Term.fromText . fst) $ toList $ kbaTokenise token of
                []    -> Nothing
                [tok] -> Just (Token tok)
                toks  -> Just (Phrase toks)

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
              fieldName <- o .: "field"
              let parseTerms :: FromJSON term => Aeson.Parser (V.Vector (term, Double))
                  parseTerms = mapM weightedTerm =<< o .: "terms"
              case fieldName :: String of
                  "freebase_id" -> do
                      model <- o .: "retrieval_model"
                      RetrievalNode <$> nodeName
                                    <*> pure model
                                    <*> pure FieldFreebaseIds
                                    <*> parseTerms
                                    <*> record
                  "text"        -> do
                      model <- o .: "retrieval_model"
                      terms <- parseTerms
                      let terms' =
                              V.fromList
                              [ (termPhrase, weight)
                              | (term, weight) <- V.toList terms
                              , Just termPhrase <- pure $ splitTerms term
                              ]
                      RetrievalNode <$> nodeName
                                    <*> pure model
                                    <*> pure FieldText
                                    <*> pure terms'
                                    <*> record
                  _             -> fail $ "Unknown field name "++fieldName
          ifNode =
              CondNode <$> o .: "terms"
                       <*> o .: "negated"
                       <*> o .: "true_child"
                       <*> o .: "false_child"

      in do ty <- o .: "type"
            case ty :: String of
              "aggregator"    -> aggregatorNode
              "constant"      -> constNode
              "drop"          -> pure DropNode
              "scale"         -> scaleNode
              "scoring_model" -> retrievalNode
              "if"            -> ifNode
              _               -> fail "Unknown node type"

collectFieldTerms :: FieldName term -> QueryNode -> [term]
collectFieldTerms _ ConstNode {}       = []
collectFieldTerms f DropNode {}        = []
collectFieldTerms f SumNode {..}       = foldMap (collectFieldTerms f) children
collectFieldTerms f ProductNode {..}   = foldMap (collectFieldTerms f) children
collectFieldTerms f ScaleNode {..}     = collectFieldTerms f child
collectFieldTerms f RetrievalNode {..}
  | Just Refl <- field `eqFieldName` f = map fst $ toList terms
  | otherwise                          = []
collectFieldTerms f CondNode {..}      = collectFieldTerms f trueChild
                                      <> collectFieldTerms f falseChild

instance FromJSON (RetrievalModel term) where
    parseJSON = withObject "retrieval model" $ \o -> do
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
    toJSON (DropNode {}) = object
        [ "type"     .= str "drop" ]
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

    toJSON (CondNode {..}) = object
        [ "type"        .= str "if"
        , "terms"       .= predicateTerms
        , "negated"     .= predicateNegated
        , "false_child" .= falseChild
        , "true_child"  .= trueChild
        ]

str :: String -> String
str = id

withName :: Maybe QueryNodeName -> [Aeson.Pair] -> [Aeson.Pair]
withName (Just name) = (("name" .= name) :)
withName _           = id

-- TODO This doesn't really belong here
kbaTokenise :: T.Text -> [(T.Text, Position)]
kbaTokenise =
    tokeniseWithPositions . T.map killPunctuation
  where
    killPunctuation c
      | c `HS.member` chars = ' '
      | otherwise           = c
      where chars = HS.fromList "\t\n\r;\"&/:!#?$%()@^*+-,=><[]{}|`~_`"
