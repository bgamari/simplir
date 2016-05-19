{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DiskIndex.Posting.Tests where

import Data.Binary (Binary)
import Control.Monad.IO.Class
import System.Directory
import System.IO.Temp
import System.IO
import qualified Data.Map as M

import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.Tasty
import Test.Tasty.QuickCheck

import Types
import DiskIndex.Posting as PostingIdx

roundTripPostings :: forall p. (Binary p, Eq p)
                  => M.Map Term (M.Map DocumentId p) -> Property
roundTripPostings postings' = monadicIO $ do
    tempDir <- liftIO getTemporaryDirectory
    (tempFile, hdl) <- liftIO $ openTempFile tempDir "postings"
    liftIO $ hClose hdl
    let postings :: M.Map Term [Posting p]
        postings = fmap (map (uncurry Posting) . M.toAscList) postings'
    liftIO $ PostingIdx.fromTermPostings 64 tempFile postings
    Right idx <- liftIO $ PostingIdx.open tempFile
    let postings'' = PostingIdx.walk idx
    liftIO $ removeFile tempFile
    return $ M.toAscList postings == postings''

tests :: TestTree
tests = testGroup "PostingIndex"
    [ testProperty "round-trip postings (Int)" (roundTripPostings @Int)
    ]
