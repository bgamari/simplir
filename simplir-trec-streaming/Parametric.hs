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
    , collectParameters
    ) where

import Control.Monad (void)
import Control.Applicative
import Data.Foldable (toList)
import Data.Aeson
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Text (Text)
import qualified Text.Trifecta as Tri

newtype Parameters a = Parameters (M.Map ParamName a)
                     deriving (Show)

instance FromJSON a => FromJSON (Parameters a) where
    parseJSON = withArray "parameters" $ fmap (Parameters . M.fromList) . mapM param . toList
      where
        param = withObject "parameter" $ \o -> (,) <$> o .: "name"
                                                   <*> o .: "value"

newtype ParamSets = ParamSets { getParamSets :: M.Map ParamSettingName (Parameters Double) }
                  deriving (Show)

instance FromJSON ParamSets where
    parseJSON = withArray "parameter sets" $ \arr ->
        ParamSets . M.fromList <$> traverse paramSet (toList arr)
      where paramSet = withObject "parameter set" $ \o ->
              (,) <$> o .: "name" <*> o .: "params"

newtype ParamSettingName = ParamSettingName Text
                         deriving (Show, Eq, Ord, ToJSON, FromJSON)

newtype ParamName = ParamName Text
                  deriving (Show, Eq, Ord, ToJSON)

instance FromJSON ParamName where
    parseJSON = withText "parameter name" $ pure . ParamName

paramName :: Tri.Parser ParamName
paramName = do
    x <- Tri.letter
    xs <- many (Tri.alphaNum <|> Tri.char '_')
    return $ ParamName $ T.pack (x:xs)

data Parametric a where
    Parameter :: ParamName -> Parametric Double
    Pure      :: a  -> Parametric a
    Ap        :: Parametric (a -> b) -> Parametric a -> Parametric b

instance Show a => Show (Parametric a) where
    show (Parameter name) = show name
    show (Pure x) = show x
    show (Ap _ _) = "\\x -> ..."

collectParameters :: Parametric a -> [ParamName]
collectParameters (Parameter p) = [p]
collectParameters (Pure _) = []
collectParameters (Ap f x) = collectParameters f ++ collectParameters x

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
            name <- paramName
            void $ Tri.string "}}"
            return $ Parameter name

instance ToJSON (Parametric Double) where
    toJSON (Pure x) = toJSON x
    toJSON (Ap _ _) = toJSON ("Uh oh, a function" :: T.Text)
    toJSON (Parameter (ParamName p)) = toJSON $ "{{"++T.unpack p++"}}"

    toEncoding (Pure x) = toEncoding x
    toEncoding (Ap _ _) = toEncoding ("Uh oh, a function" :: T.Text)
    toEncoding (Parameter (ParamName p)) = toEncoding $ "{{"++T.unpack p++"}}"
    {-# INLINE toEncoding #-}

