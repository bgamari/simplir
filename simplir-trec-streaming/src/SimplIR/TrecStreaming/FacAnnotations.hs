{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.TrecStreaming.FacAnnotations where

import Pipes
import Pipes.Attoparsec as P.A
import qualified Data.Text as T
import Data.Text (Text)
import Data.Attoparsec.Text as A
import SimplIR.Types
import Data.Binary
import Data.Hashable

data Document = Document { docArchive :: Text
                         , docId      :: Text
                         , docAnnotations :: [Annotation]
                         }
              deriving (Show)

data Annotation = Annotation { annSurfaceForm      :: Text
                             , annSpan             :: Span
                             , annPosterior        :: Double
                               -- ^ Confidence given document context and surface form
                             , annPosteriorContext :: Double
                               -- ^ Confidence given only document context
                             , annEntity            :: EntityId
                             }
                deriving (Show)

newtype EntityId = EntityId {getEntityId :: Text}
                 deriving (Eq, Ord, Show, Binary, Hashable)

parseDocument :: Parser Document
parseDocument = do
    docArchive <- A.takeWhile (/= '#') <* char '#'
    docId <- A.takeWhile (/= '\n')
    docAnnotations <- manyTill (endOfLine *> annotation) (endOfLine >> endOfLine)
    return Document {..}

tab :: Parser ()
tab = char '\t' >> return ()

annotation :: Parser Annotation
annotation = do
    annSurfaceForm <- A.takeWhile (/= '\t') <* tab
    annSpan <- Span <$> decimal <* char '\t' <*> decimal <* tab
    annPosterior <- double <* tab
    annPosteriorContext <- double <* tab
    annEntity <- EntityId <$> A.takeWhile (/= '\n')
    return Annotation {..}

parseDocuments :: Monad m
               => Producer T.Text m r
               -> Producer Document m (Either (ParsingError, Producer T.Text m r) r)
parseDocuments = P.A.parsed parseDocument
