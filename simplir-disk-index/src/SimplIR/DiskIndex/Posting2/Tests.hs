{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.DiskIndex.Posting2.Tests where

import Codec.Serialise (Serialise)
import Control.Monad.IO.Class
import System.Directory
import System.IO.Temp
import System.IO
import qualified Data.Map as M

import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.Tasty
import Test.Tasty.QuickCheck

import SimplIR.Term
import SimplIR.Types
import SimplIR.DiskIndex.Posting2 as PostingIdx

roundTripPostings :: forall p. (Serialise p, Eq p)
                  => M.Map Term (M.Map DocumentId p) -> Property
roundTripPostings postings' = monadicIO $ do
    tempDir <- liftIO getTemporaryDirectory
    (tempFile, hdl) <- liftIO $ openTempFile tempDir "postings"
    liftIO $ hClose hdl
    let postings :: M.Map Term [Posting p]
        postings = fmap (map (uncurry Posting) . M.toAscList) postings'
    idxFile <- liftIO $ PostingIdx.fromTermPostings 64 tempFile postings
    idx <- liftIO $ PostingIdx.open idxFile
    let postings'' = PostingIdx.toPostingsLists idx
    liftIO $ removeFile tempFile
    return $ M.toAscList postings == postings''

tests :: TestTree
tests = testGroup "PostingIndex2"
    [ testProperty "round-trip postings (Int)" (roundTripPostings @Int)
    ]
