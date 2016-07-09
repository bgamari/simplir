{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

module Parametric
    ( Parameters(..)
    , ParamSets(..)
    , ParamSettingName(..)
    , ParamName(..)
    , Parametric(..)
    , runParametric
    , runParametricOrFail
    ) where

import Control.Monad (void)
import Control.Applicative
import Data.Foldable (toList)
import Data.Aeson
import Data.Bifunctor
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import Data.Text (Text)
import qualified Text.Trifecta as Tri

newtype Parameters a = Parameters (M.Map ParamName a)
                     deriving (Show)

instance FromJSON a => FromJSON (Parameters a) where
    parseJSON = withObject "parameters" $ \o ->
        Parameters . M.fromList <$> traverse (traverse parseJSON . first ParamName) (HM.toList o)

newtype ParamSets = ParamSets { getParamSets :: M.Map ParamSettingName (Parameters Double) }
                  deriving (Show)

instance FromJSON ParamSets where
    parseJSON = withArray "parameter sets" $ \arr ->
        ParamSets . M.fromList <$> traverse paramSet (toList arr)
      where paramSet = withObject "parameter set" $ \o ->
              (,) <$> o .: "name" <*> o .: "values"

newtype ParamSettingName = ParamSettingName Text
                         deriving (Show, Eq, Ord, ToJSON, FromJSON)

newtype ParamName = ParamName Text
                  deriving (Show, Eq, Ord, ToJSON, FromJSON)

data Parametric a where
    Parameter :: ParamName -> Parametric Double
    Pure      :: a  -> Parametric a
    Ap        :: Parametric (a -> b) -> Parametric a -> Parametric b

runParametric :: Parameters Double -> Parametric a -> Either ParamName a
runParametric (Parameters params) = go
  where
    go :: Parametric a -> Either ParamName a
    go (Parameter param)
      | Just val <- M.lookup param params = Right val
      | otherwise = Left param
    go (Pure x) = Right x
    go (Ap f x) = go f <*> go x

runParametricOrFail :: Parameters Double -> Parametric a -> a
runParametricOrFail params = either uhOh id . runParametric params
  where
    uhOh (ParamName missingParam) =
        error ("Missing parameter value: " ++ T.unpack missingParam)

instance Functor Parametric where
    fmap f (Pure x) = Pure (f x)
    fmap f x        = Pure f `Ap` x

instance Applicative Parametric where
    pure = Pure
    (<*>) = Ap

instance FromJSON (Parametric Double) where
    parseJSON o = fixed o <|> param o
      where
        fixed = withScientific "parameter value" $ pure . Pure . realToFrac
        param = withText "parameter expression" $ \expr ->
            case Tri.parseString parse mempty (T.unpack expr) of
              Tri.Success a   -> pure a
              Tri.Failure err -> fail $ show err
        parse = do
            void $ Tri.string "{{"
            param <- fmap (ParamName . T.pack) $ do
                x <- Tri.letter
                xs <- many Tri.alphaNum
                return (x:xs)
            void $ Tri.string "}}"
            return $ Parameter param

instance ToJSON (Parametric Double) where
    toJSON (Pure x) = toJSON x
    toJSON (Ap _ _) = "Uh oh, a function"
    toJSON (Parameter (ParamName p)) = toJSON $ "{{"++T.unpack p++"}}"

