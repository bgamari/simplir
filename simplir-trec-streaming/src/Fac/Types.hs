{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}

module Fac.Types where

import qualified Data.Text as T
import qualified Data.Map.Strict as M
import Data.Semigroup
import Data.Binary
import GHC.Generics
import System.FilePath

import qualified BTree.File as BTree
import SimplIR.BinaryFile as BinaryFile
import SimplIR.Types
import SimplIR.TrecStreaming.FacAnnotations (EntityId)

data DocumentInfo = DocInfo { docArchive :: ArchiveName
                            , docName    :: DocumentName
                            , docLength  :: DocumentLength
                            }
                  deriving (Generic, Eq, Ord, Show)
instance Binary DocumentInfo

newtype DocumentFrequency = DocumentFrequency Int
                          deriving (Show, Eq, Ord, Binary)

instance Semigroup DocumentFrequency where
    DocumentFrequency a <> DocumentFrequency b = DocumentFrequency (a+b)

instance Monoid DocumentFrequency where
    mempty = DocumentFrequency 0
    mappend = (<>)

type ArchiveName = T.Text

data CorpusStats = CorpusStats { corpusCollectionLength :: !Int
                                 -- ^ How many tokens in collection
                               , corpusCollectionSize   :: !Int
                                 -- ^ How many documents in collection
                               }
                 deriving (Generic)

instance Binary CorpusStats

instance Semigroup CorpusStats where
    a <> b =
        CorpusStats { corpusCollectionLength = corpusCollectionLength a + corpusCollectionLength b
                    , corpusCollectionSize = corpusCollectionSize a + corpusCollectionSize b
                    }

instance Monoid CorpusStats where
    mempty = CorpusStats 0 0
    mappend = (<>)

diskIndexPaths :: FilePath -> DiskIndex
diskIndexPaths root =
    DiskIndex { diskRootDir     = root
              , diskDocuments   = BTree.BTreePath $ root </> "documents"
              , diskTermStats   = BTree.BTreePath $ root </> "term-stats"
              , diskCorpusStats = BinaryFile.BinaryFile $ root </> "corpus-stats"
              }

data TermStats = TermStats !TermFrequency !DocumentFrequency
               deriving (Show, Generic)
instance Binary TermStats
instance Semigroup TermStats where
    TermStats a b <> TermStats c d = TermStats (a <> c) (b <> d)
instance Monoid TermStats where
    mempty = TermStats mempty mempty
    mappend = (<>)

-- | Paths to the parts of an index on disk
data DiskIndex = DiskIndex { diskRootDir     :: FilePath
                           , diskDocuments   :: BTree.BTreePath DocumentName (DocumentInfo, M.Map EntityId TermFrequency)
                           , diskTermStats   :: BTree.BTreePath EntityId TermStats
                           , diskCorpusStats :: BinaryFile CorpusStats
                           }

type FragmentIndex p =
    ( M.Map DocumentName (DocumentInfo, M.Map EntityId p)
    , M.Map EntityId TermStats
    , CorpusStats
    )
