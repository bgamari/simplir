{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Types where

import Data.Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Bifunctor
import Data.Function (on)
import Data.Foldable (toList)
import Data.Binary
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import GHC.Generics
import Numeric.Log

import           SimplIR.Term (Term)
import           SimplIR.Types
import           SimplIR.RetrievalModels.QueryLikelihood (Score)
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac
import Query

data Ranking = Ranking { rankingQueryId :: QueryId
                       , rankingResults :: [ScoredDocument]
                       }
             deriving (Show)

instance ToJSON Ranking where
    toJSON Ranking{..} = object
        [ "query_id" .= rankingQueryId
        , "results"  .= rankingResults
        ]

instance FromJSON Ranking where
    parseJSON = withObject "ranking" $ \o ->
        Ranking <$> o .: "query_id" <*> o .: "results"

data ScoredDocument = ScoredDocument { scoredRankScore      :: !Score
                                     , scoredDocumentInfo   :: !DocumentInfo
                                     , scoredTermPositions  :: !(M.Map Term [Position])
                                     , scoredEntityFreqs    :: !(M.Map Fac.EntityId TermFrequency)
                                     , scoredRecordedValues :: !(M.Map RecordedValueName Aeson.Value)
                                     }
                    deriving (Show)

instance Ord ScoredDocument where
    compare = compare `on` scoredRankScore

instance Eq ScoredDocument where
    (==) = (==) `on` scoredRankScore

instance ToJSON ScoredDocument where
    toJSON ScoredDocument{scoredDocumentInfo=DocInfo{..}, ..} = object
        [ "doc_name"     .= docName
        , "length"       .= docLength
        , "archive"      .= docArchive
        , "score"        .= ln scoredRankScore
        , "postings"     .= [ object ["term" .= term, "positions" .= positions]
                            | (term, positions) <- M.toAscList scoredTermPositions
                            ]
        , "entities"     .= object (map (bimap Fac.getEntityId toJSON) $ M.toAscList scoredEntityFreqs)
        , "recorded_values" .= object
          [ valueName .= value
          | (RecordedValueName valueName, value) <- M.assocs scoredRecordedValues
          ]
        ]

instance FromJSON ScoredDocument where
    parseJSON = withObject "ScoredDocument" $ \o -> do
        docName <- o .: "doc_name"
        docLength <- o .: "length"
        docArchive <- o .: "archive"
        postings <- o .: "postings"
        recordedValues <- o .: "recorded_values" >>= parseMap (pure . RecordedValueName) parseJSON
        ScoredDocument
            <$> fmap Exp (o .: "score")
            <*> pure (DocInfo {..})
            <*> withArray "postings" parsePostings postings
            <*> pure mempty -- fmap parseEntity (o .: "entities") -- TODO
            <*> pure recordedValues
      where
        parsePostings = fmap M.unions . traverse parsePosting . toList
        parsePosting = withObject "posting" $ \o ->
            M.singleton <$> o .: "term" <*> o .: "positions"
        -- parseEntity = withObject "posting" $ \o -> HM.toList

type ArchiveName = T.Text

data DocumentInfo = DocInfo { docArchive :: !ArchiveName
                            , docName    :: !DocumentName
                            , docLength  :: !DocumentLength
                            }
                  deriving (Generic, Eq, Ord, Show)
instance Binary DocumentInfo

parseMap :: Ord k
         => (T.Text -> Aeson.Parser k)
         -> (Value -> Aeson.Parser a)
         -> Object
         -> Aeson.Parser (M.Map k a)
parseMap parseKey parseValue o =
    M.unions <$> traverse (\(k,v) -> M.singleton <$> parseKey k <*> parseValue v) (HM.toList o)
