{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
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
    , QLSmoothingMethod(..), toSmoothing
    , WikiId(..)
      -- * Features
    , FeatureName(..)
    , recordedFeatureName
    , featureParameterName
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

newtype FeatureName = FeatureName Text
                    deriving (Show, Eq, Ord, ToJSON, FromJSON)

recordedFeatureName :: FeatureName -> RecordedValueName
recordedFeatureName (FeatureName fname) = RecordedValueName fname

featureParameterName :: FeatureName -> ParamName
featureParameterName (FeatureName fname) = ParamName fname

data FieldName a where
    FieldFreebaseIds :: FieldName Fac.EntityId
    FieldText        :: FieldName (TokenOrPhrase Term)

instance ToJSON (FieldName a) where
    toJSON FieldText        = toJSON $ str "text"
    toJSON FieldFreebaseIds = toJSON $ str "freebase_id"

eqFieldName :: FieldName a -> FieldName b -> Maybe (a :~: b)
eqFieldName FieldFreebaseIds FieldFreebaseIds = Just Refl
eqFieldName FieldText FieldText = Just Refl
eqFieldName _ _ = Nothing

deriving instance Show a => Show (FieldName a)
deriving instance Eq (FieldName a)

data QLSmoothingMethod
    = SmoothNone
    | SmoothDirichlet { dirichletMu :: Parametric Double }
    | SmoothJelinekMercer { jmAlphaFore, jmAlphaBack :: Parametric Double }
    deriving (Show)

instance FromJSON QLSmoothingMethod where
    parseJSON = withObject "smoothing" $ \s -> do
        smoothingType <-  s .: "type"
        case smoothingType :: String of
            "none"      -> pure SmoothNone
            "dirichlet" -> SmoothDirichlet <$> s .: "mu"
            "jm"        -> SmoothJelinekMercer <$> s .: "alpha_foreground"
                                               <*> s .: "alpha_background"
            _           -> fail $ "Unknown smoothing method "++smoothingType

instance ToJSON QLSmoothingMethod where
    toJSON SmoothNone = object [ "type" .= str "none" ]
    toJSON SmoothDirichlet{..} = object [ "type" .= str "dirichlet"
                                        , "mu"   .= dirichletMu
                                        ]
    toJSON SmoothJelinekMercer{..} = object [ "type" .= str "jm"
                                            , "alpha_foreground" .= jmAlphaFore
                                            , "alpha_background" .= jmAlphaBack
                                            ]

toSmoothing :: (forall a. Parametric a -> a)
            -> Distribution term -> QLSmoothingMethod -> Smoothing term
toSmoothing _    _       SmoothNone =
   NoSmoothing
toSmoothing runP bgDist (SmoothDirichlet mu) =
   Dirichlet (realToFrac $ runP mu) bgDist
toSmoothing runP bgDist (SmoothJelinekMercer fg bg) =
   JelinekMercer alpha bgDist
  where fg' = runP fg
        bg' = runP bg
        alpha = realToFrac $ fg' / (fg' + bg')

data RetrievalModel
    = QueryLikelihood QLSmoothingMethod
    deriving (Show)

instance FromJSON RetrievalModel where
    parseJSON = withObject "retrieval model" $ \o -> do
        modelType <- o .: "type"
        case modelType of
          "ql" -> QueryLikelihood <$> o .: "smoothing"
          _    -> fail $ "Unknown retrieval model "++modelType

instance ToJSON RetrievalModel where
    toJSON (QueryLikelihood smoothing) = object
        [ "type"      .= str "ql"
        , "smoothing" .= smoothing
        ]

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
                 -- | A feature for learning-to-rank. The value of the child node is recorded as a feature and the
                 -- weight is loaded from the parameter set. Both are identified by 'featureName'.
               | FeatureNode { featureName  :: FeatureName
                             , child        :: QueryNode
                             }
               | forall term. (Show term, ToJSON term, FromJSON term) =>
                 RetrievalNode { name           :: Maybe QueryNodeName
                               , retrievalModel :: RetrievalModel
                               , field          :: FieldName term
                               , terms          :: V.Vector (term, Double)
                               , recordOutput   :: Maybe RecordedValueName
                               }

               | CondNode { predicateTerms   :: V.Vector (TokenOrPhrase Term)
                          , predicateNegated :: Bool
                          , trueChild        :: QueryNode
                          , falseChild       :: QueryNode
                          }

deriving instance Show QueryNode

instance FromJSON QueryNode where
    parseJSON = withObject "query node" $ \o ->
      let nodeName = fmap QueryNodeName <$> o .:? "name"

          weightedTerm :: FromJSON term => Aeson.Value -> Aeson.Parser (term, Double)
          weightedTerm val = weighted val <|> unweighted
            where
              unweighted = ((\x -> (x, 1)) <$> parseJSON val)
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

          featureNode = FeatureNode
              <$> o .: "name"
              <*> o .: "child"

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
                       <*> o .:? "negated" .!= False
                       <*> o .: "true_child"
                       <*> o .: "false_child"

      in do ty <- o .: "type"
            case ty :: String of
              "aggregator"    -> aggregatorNode
              "constant"      -> constNode
              "drop"          -> pure DropNode
              "scale"         -> scaleNode
              "feature"       -> featureNode
              "scoring_model" -> retrievalNode
              "if"            -> ifNode
              _               -> fail "Unknown node type"

collectFieldTerms :: FieldName term -> QueryNode -> [term]
collectFieldTerms _ ConstNode {}       = []
collectFieldTerms _ DropNode {}        = []
collectFieldTerms f SumNode {..}       = foldMap (collectFieldTerms f) children
collectFieldTerms f ProductNode {..}   = foldMap (collectFieldTerms f) children
collectFieldTerms f ScaleNode {..}     = collectFieldTerms f child
collectFieldTerms f FeatureNode {..}   = collectFieldTerms f child
collectFieldTerms f RetrievalNode {..}
  | Just Refl <- field `eqFieldName` f = map fst $ toList terms
  | otherwise                          = []
collectFieldTerms f CondNode {..}      = collectFieldTerms f trueChild
                                      <> collectFieldTerms f falseChild

instance ToJSON QueryNode where
    toJSON (ConstNode {..}) = object
        [ "type"     .= str "constant"
        , "value"    .= value
        ]
    toJSON (DropNode {}) = object
        [ "type"          .= str "drop" ]
    toJSON (SumNode {..}) = object
        $ withName name
        [ "type"          .= str "aggregator"
        , "op"            .= str "sum"
        , "children"      .= children
        , "record_output" .= recordOutput
        ]
    toJSON (ProductNode {..}) = object
        $ withName name
        [ "type"          .= str "aggregator"
        , "op"            .= str "product"
        , "children"      .= children
        , "record_output" .= recordOutput
        ]
    toJSON (ScaleNode {..}) = object
        $ withName name
        [ "type"          .= str "weight"
        , "child"         .= child
        , "record_output" .= recordOutput
        ]
    toJSON (FeatureNode {..}) = object
        [ "type"          .= str "feature"
        , "name"          .= featureName
        , "child"         .= child
        ]
    toJSON (RetrievalNode {..}) = object
        $ withName name
        [ "type"          .= str "retrieval_model"
        , "model"         .= retrievalModel
        , "field"         .= field
        , "terms"         .= terms
        , "record_output" .= recordOutput
        ]

    toJSON (CondNode {..}) = object
        [ "type"          .= str "if"
        , "terms"         .= predicateTerms
        , "negated"       .= predicateNegated
        , "false_child"   .= falseChild
        , "true_child"    .= trueChild
        ]

str :: String -> String
str = id

withName :: Maybe QueryNodeName -> [Aeson.Pair] -> [Aeson.Pair]
withName (Just name) = (("name" .= name) :)
withName _           = id

-- TODO This doesn't really belong here
kbaTokenise :: T.Text -> [(T.Text, Position)]
kbaTokenise =
    tokeniseWithPositions . killPunctuation
