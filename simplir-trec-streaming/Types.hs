{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}

module Types where

import qualified Data.Text as T
import qualified Data.Map.Strict as M
import Data.Binary
import GHC.Generics
import System.FilePath

import qualified BTree.File as BTree
import SimplIR.Term as Term
import SimplIR.BinaryFile as BinaryFile
import SimplIR.Types
import qualified SimplIR.TrecStreaming.FacAnnotations as Fac
import SimplIR.TrecStreaming.FacAnnotations (EntityId)

data DocumentInfo = DocInfo { docArchive :: ArchiveName
                            , docName    :: DocumentName
                            , docLength  :: DocumentLength
                            }
                  deriving (Generic, Eq, Ord, Show)
instance Binary DocumentInfo

newtype DocumentFrequency = DocumentFrequency Int
                          deriving (Show, Eq, Ord, Binary)
instance Monoid DocumentFrequency where
    mempty = DocumentFrequency 0
    DocumentFrequency a `mappend` DocumentFrequency b = DocumentFrequency (a+b)

type ArchiveName = T.Text

data CorpusStats = CorpusStats { corpusCollectionLength :: !Int
                                 -- ^ How many tokens in collection
                               , corpusCollectionSize   :: !Int
                                 -- ^ How many documents in collection
                               }
                 deriving (Generic)

instance Binary CorpusStats
instance Monoid CorpusStats where
    mempty = CorpusStats 0 0
    a `mappend` b =
        CorpusStats { corpusCollectionLength = corpusCollectionLength a + corpusCollectionLength b
                    , corpusCollectionSize = corpusCollectionSize a + corpusCollectionSize b
                    }

diskIndexPaths :: FilePath -> DiskIndex
diskIndexPaths root =
    DiskIndex { diskRootDir     = root
              , diskDocuments   = BTree.BTreePath $ root </> "documents"
              , diskTermStats   = BTree.BTreePath $ root </> "term-stats"
              , diskCorpusStats = BinaryFile.BinaryFile $ root </> "corpus-stats"
              }

-- | Paths to the parts of an index on disk
data DiskIndex = DiskIndex { diskRootDir     :: FilePath
                           , diskDocuments   :: BTree.BTreePath DocumentName (DocumentInfo, M.Map EntityId TermFrequency)
                           , diskTermStats   :: BTree.BTreePath EntityId (TermFrequency, DocumentFrequency)
                           , diskCorpusStats :: BinaryFile CorpusStats
                           }

type FragmentIndex p =
    ( M.Map DocumentName (DocumentInfo, M.Map EntityId p)
    , M.Map EntityId (TermFrequency, DocumentFrequency)
    , CorpusStats
    )
