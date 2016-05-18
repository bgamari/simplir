{-# LANGUAGE TypeApplications #-}

module DiskIndex.Tests where

import Control.Monad.IO.Class
import Data.Traversable (forM)
import Data.Foldable (forM_)
import Data.Tuple (swap)
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified DiskIndex
import Types

import Test.QuickCheck
import Test.QuickCheck.Modifiers
import Test.QuickCheck.Monadic
import Test.Tasty
import Test.Tasty.QuickCheck

import System.Directory
import System.IO.Temp
import System.FilePath

mergeContainsAll :: [M.Map DocumentName (M.Map Term (Positive Int))] -> Property
mergeContainsAll chunks = monadicIO $ do
    tempDir <- liftIO $ getTemporaryDirectory >>= (\tmp -> createTempDirectory tmp "index")
    liftIO $ print ("mergeContainsAll", tempDir)

    -- uniquify document names
    --let chunks = zipWith uniqueDocName [0..] chunks'
    --    uniqueDocName n = M.mapKeys (\(DocName name) -> DocName $ "chunk-"++show n++":"++name)

    -- generate chunk indexes
    indexDirs <- liftIO $ forM (zip [0..] chunks) $ \(i,docs) -> do
        let indexDir = tempDir </> "index-"++show i
        let postings :: M.Map Term [Posting Int]
            postings = M.fromListWith mappend
                [ (term, [Posting docId tf])
                | (docName, terms) <- M.assocs docs
                , (term, Positive tf) <- M.assocs terms
                , let docId = docMap M.! docName
                ]
            docMap :: M.Map DocumentName DocumentId
            docMap = M.fromList $ zip (M.keys docs) [DocId 0..]
        print ("generated", indexDir, postings)
        DiskIndex.fromDocuments indexDir (map swap $ M.assocs docMap) postings
        return indexDir

    -- merge indexes
    liftIO $ print ("indexes", indexDirs)
    indexes <- liftIO $ mapM (DiskIndex.open @DocumentName @Int) indexDirs
    liftIO $ print ("indexes", fmap (fmap DiskIndex.termPostings) (zip indexDirs indexes))
    let indexDir = tempDir </> "merged"
    liftIO $ DiskIndex.merge @DocumentName @Int indexDir indexes
    liftIO $ mapM_ removeDirectoryRecursive indexDirs

    -- check merged index
    let gold :: M.Map Term (M.Map DocumentName Int)
        gold = M.unionsWith mappend
               [ M.singleton term (M.singleton docName n)
               | chunk <- chunks
               , (docName, terms) <- M.assocs chunk
               , (term, Positive n) <- M.assocs terms
               ]
    merged <- liftIO $ DiskIndex.open indexDir
    let test :: M.Map Term (M.Map DocumentName Int)
        test = M.unionsWith mappend
               [ M.singleton term (M.singleton docName n)
               | (term, postings) <- DiskIndex.termPostings merged
               , Posting docId n <- postings
               , let Just docName = DiskIndex.lookupDoc docId merged
               ]
    liftIO $ print (chunks, test)
    liftIO $ removeDirectoryRecursive tempDir
    return $ gold == test

tests :: TestTree
tests = testGroup "DiskIndex"
    [ testProperty "merge contains all" mergeContainsAll
    ]
