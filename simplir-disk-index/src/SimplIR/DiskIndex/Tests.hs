{-# LANGUAGE TypeApplications #-}

module SimplIR.DiskIndex.Tests where

import Data.List (sort)
import Control.Monad.IO.Class
import Data.Traversable (forM)
import Data.Tuple (swap)
import qualified Data.Map.Strict as M

import qualified SimplIR.DiskIndex as DiskIndex
import           SimplIR.Term
import           SimplIR.Types

import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.Tasty
import Test.Tasty.QuickCheck

import System.Directory
import System.IO.Temp
import System.FilePath

mergeContainsAll :: [M.Map DocumentName (M.Map Term (Positive Int))] -> Property
mergeContainsAll chunks' = monadicIO $ do
    tempDir <- liftIO $ getTemporaryDirectory >>= (\tmp -> createTempDirectory tmp "index")

    -- uniquify document names
    --let chunks = zipWith uniqueDocName [0..] chunks'
    --    uniqueDocName n = M.mapKeys (\(DocName name) -> DocName $ "chunk-"++show n++":"++name)
    let chunks = map (M.filter (not . M.null)) chunks'

    -- generate chunk indexes
    indexDirs <- liftIO $ forM (zip [0::Int ..] chunks) $ \(i,docs) -> do
        let indexDir = tempDir </> "index-"++show i
        let postings :: M.Map Term [Posting Int]
            postings = fmap sort $ M.fromListWith mappend
                [ (term, [Posting docId tf])
                | (docName, terms) <- M.assocs docs
                , (term, Positive tf) <- M.assocs terms
                , let docId = docMap M.! docName
                ]
            docMap :: M.Map DocumentName DocumentId
            docMap = M.fromList $ zip (M.keys docs) [DocId 0..]
        DiskIndex.fromDocuments indexDir (map swap $ M.assocs docMap) postings

    -- merge indexes
    indexes <- liftIO $ mapM (DiskIndex.open @Term @DocumentName @Int) indexDirs
    let mergedPath = tempDir </> "merged"
    merged <- liftIO $ DiskIndex.merge @Term @DocumentName @Int mergedPath indexDirs
    liftIO $ mapM_ (removeDirectoryRecursive . DiskIndex.getDiskIndexPath) indexDirs

    -- check merged index
    let gold :: M.Map Term (M.Map DocumentName Int)
        gold = M.unionsWith mappend
               [ M.singleton term (M.singleton docName n)
               | chunk <- chunks
               , (docName, terms) <- M.assocs chunk
               , (term, Positive n) <- M.assocs terms
               ]
    merged <- liftIO $ DiskIndex.open merged
    let test :: M.Map Term (M.Map DocumentName Int)
        test = M.unionsWith mappend
               [ M.singleton term (M.singleton docName n)
               | (term, postings) <- DiskIndex.termPostings merged
               , Posting docId n <- postings
               , let Just docName = DiskIndex.lookupDoc docId merged
               ]
    liftIO $ removeDirectoryRecursive tempDir
    return $ (gold === test :: Property)

tests :: TestTree
tests = testGroup "DiskIndex"
    [ testProperty "merge contains all" mergeContainsAll
    ]
