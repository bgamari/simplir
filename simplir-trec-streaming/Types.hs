{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Types where

import Data.Aeson
import Data.Bifunctor
import Data.Foldable (toList)
import Data.Binary
import qualified Data.Map as M
import qualified Data.Text as T
import GHC.Generics
import Numeric.Log

import           SimplIR.Term (Term)
import           SimplIR.Types
import           SimplIR.RetrievalModels.QueryLikelihood (Score)
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac

type QueryId = T.Text

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

data ScoredDocument = ScoredDocument { scoredRankScore     :: !Score
                                     , scoredDocumentInfo  :: !DocumentInfo
                                     , scoredTermPositions :: !(M.Map Term [Position])
                                     , scoredTermScore     :: !Score
                                     , scoredEntityFreqs   :: !(M.Map Fac.EntityId TermFrequency)
                                     , scoredEntityScore   :: !Score
                                     }
                    deriving (Show, Ord, Eq)

instance ToJSON ScoredDocument where
    toJSON ScoredDocument{scoredDocumentInfo=DocInfo{..}, ..} = object
        [ "doc_name"     .= docName
        , "length"       .= docLength
        , "archive"      .= docArchive
        , "score"        .= ln scoredRankScore
        , "term_score"        .= ln scoredTermScore
        , "postings"     .= [ object ["term" .= term, "positions" .= positions]
                            | (term, positions) <- M.toAscList scoredTermPositions
                            ]
        , "entity_score" .= ln scoredEntityScore
        , "entities"     .= object (map (bimap Fac.getEntityId toJSON) $ M.toAscList scoredEntityFreqs)
        ]

instance FromJSON ScoredDocument where
    parseJSON = withObject "ScoredDocument" $ \o -> do
        docName <- o .: "doc_name"
        docLength <- o .: "length"
        docArchive <- o .: "archive"
        postings <- o .: "postings"
        ScoredDocument
            <$> fmap Exp (o .: "score")
            <*> pure (DocInfo {..})
            <*> withArray "postings" parsePostings postings
            <*> pure 0 -- <*> fmap Exp (o .: "term_score")
            <*> pure mempty -- fmap parseEntity (o .: "entities") -- TODO
            <*> pure 0 -- fmap Exp (o .: "entity_score")
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
