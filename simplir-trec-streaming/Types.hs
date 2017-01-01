{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}

module Types where

import Data.Aeson
import qualified Data.Aeson.Encoding.Internal as Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Bifunctor
import Data.Function (on)
import Data.Traversable
import Data.Foldable (toList)
import Data.Binary
import Data.Monoid
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as VU
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import GHC.Generics
import Numeric.Log

import           SimplIR.Term (Term)
import           SimplIR.Types
import           SimplIR.RetrievalModels.QueryLikelihood (Score)
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac
import Query
import Parametric

newtype Results = Results (M.Map (QueryId, ParamSettingName) [ScoredDocument])
                deriving (Show)

instance ToJSON Results where
    toEncoding (Results results) =
        Aeson.pairs $ foldMap queryPairs $ M.toList results'
      where
        queryPairs (QueryId qid, psets) =
            qid `Aeson.pair` Aeson.pairs (foldMap paramPairs $ M.toList psets)

        paramPairs :: (ParamSettingName, [ScoredDocument]) -> Aeson.Series
        paramPairs (ParamSettingName pset, docs) = pset Aeson..= docs

        results' = M.unionsWith M.union
            [ M.singleton qid (M.singleton pset docs)
            | ((qid, pset), docs) <- M.toList results
            ]


    toJSON (Results results) =
        Aeson.object $ map queryPairs $ M.toList results'
      where
        queryPairs (QueryId qid, psets) =
            qid Aeson..= Aeson.object (map paramPairs $ M.toList psets)

        paramPairs :: (ParamSettingName, [ScoredDocument]) -> Aeson.Pair
        paramPairs (ParamSettingName pset, docs) = pset Aeson..= docs

        results' = M.unionsWith M.union
            [ M.singleton qid (M.singleton pset docs)
            | ((qid, pset), docs) <- M.toList results
            ]

instance FromJSON Results where
    parseJSON = withObject "queries" $ \queries ->
      fmap (Results . M.unions) $ forM (HM.toList queries) $ \(qid, params) ->
        case params of
            Object obj ->
                fmap M.unions $ forM (HM.toList obj) $ \(pset, sdoc) ->
                    M.singleton (QueryId qid, ParamSettingName pset) <$> parseJSON sdoc
            _ -> fail "Results: Expected parameter sets"

data ScoredDocument = ScoredDocument { scoredRankScore      :: !Score
                                     , scoredDocumentInfo   :: !DocumentInfo
                                       -- these are left lazy to reduce intermediate allocations of these maps since
                                       -- they are filtered from the document postings
                                     , scoredTermPositions  :: M.Map (TokenOrPhrase Term) (VU.Vector Position)
                                     , scoredEntityFreqs    :: M.Map Fac.EntityId TermFrequency
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
    toEncoding ScoredDocument{scoredDocumentInfo=DocInfo{..}, ..} = pairs
         $ "doc_name"     .= docName
        <> "length"       .= docLength
        <> "archive"      .= docArchive
        <> "score"        .= ln scoredRankScore
        <> "postings"     .= [ object ["term" .= term, "positions" .= positions]
                             | (term, positions) <- M.toAscList scoredTermPositions
                             ]
        <> "entities"  `Aeson.pair` pairs (foldMap (\(ent,freq) -> Fac.getEntityId ent .= freq)
                                             (M.toAscList scoredEntityFreqs))
        <> "recorded_values" .= object
           [ valueName .= value
           | (RecordedValueName valueName, value) <- M.assocs scoredRecordedValues
           ]

instance FromJSON ScoredDocument where
    parseJSON = withObject "ScoredDocument" $ \o -> do
        docName <- o .: "doc_name"
        docLength <- o .: "length"
        docArchive <- o .: "archive"
        postings <- o .: "postings"
        entities <- o .: "entities"
        recordedValues <- o .: "recorded_values" >>= parseMap (pure . RecordedValueName) parseJSON
        ScoredDocument
            <$> fmap Exp (o .: "score")
            <*> pure (DocInfo {..})
            <*> withArray "postings" parsePostings postings
            <*> parseEntities entities
            <*> pure recordedValues
      where
        parsePostings = fmap M.unions . traverse parsePosting . toList
        parsePosting = withObject "posting" $ \o ->
            M.singleton <$> o .: "term" <*> o .: "positions"
        parseEntities = withObject "posting" $ \o ->
            M.fromList <$> traverse (traverse parseJSON . first Fac.EntityId) (HM.toList o)

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
