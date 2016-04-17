{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Data.List (sort)
import Control.Monad.State.Strict hiding ((>=>))
import Data.Bifunctor
import Data.Foldable
import Data.Profunctor
import Data.Monoid

import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.HashSet as HS
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy as T.L
import qualified Data.Vector.Unboxed as VU
import qualified Control.Foldl as Foldl

import           Pipes
import qualified Pipes.Prelude as P.P

import Data.Warc as Warc
import qualified SimplIR.HTML.Clean as Clean
import qualified BTree.BinaryList as BTree.BL
import Data.Binary (Binary)

import Utils
import Types
import Progress
import Tokenise
import WarcDocSource
import AccumPostings
import DataSource
import DiskIndex

dsrc = S3Object { s3Bucket = "aws-publicdatasets"
                , s3Object = "common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"
                }
--dsrc = LocalFile "../0000tw-00.warc"
compression = Just GZip

main :: IO ()
main = do
    compProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    expProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    pollProgress compProg 1 $ \(Sum n) -> "Compressed "++show (realToFrac n / 1024 / 1024)
    pollProgress expProg 1 $ \(Sum n) -> "Expanded "++show (realToFrac n / 1024 / 1024)

    takeTerms <- HS.fromList . map (Term . T.toCaseFold) . T.words <$> TIO.readFile "terms"
    let normTerms :: [(Term, p)] -> [(Term, p)]
        normTerms = filterTerms . caseNorm
          where
            caseNorm = map (first toCaseFold)
            filterTerms = filter (\(k,_) -> k `HS.member` takeTerms)

    withDataSource dsrc $ \src -> do
        let warc :: Warc.Warc IO ()
            warc = Warc.parseWarc
                   $ decompress compression (src >-> progressPipe compProg (Sum . BS.length))
                   >-> progressPipe expProg (Sum . BS.length)

        (docIds, postings) <-
                consumePostings
             $  void (Warc.produceRecords readRecord warc)
            >-> cat'                                          @(Warc.RecordHeader, BS.L.ByteString)
            >-> decodeDocuments
            >-> cat'                                          @(DocumentName, T.Text)
            >-> P.P.map (fmap cleanHtml)
            >-> cat'                                          @(DocumentName, T.Text)
            >-> P.P.map (fmap tokeniseWithPositions)
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> P.P.map (fmap normTerms)
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> zipWithList [DocId 0..]
            >-> cat'                                          @(DocumentId, (DocumentName, [(Term, Position)]))
            >-> P.P.map (\(docId, (docName, postings)) ->
                          ((docId, docName),
                           toPostings docId
                           $ M.assocs
                           $ foldTokens accumPositions postings))
            >-> cat'                                          @((DocumentId, DocumentName), TermPostings (VU.Vector Position))

        let postings' :: SavedPostings [Position]
            postings' = fmap (sort . map (fmap VU.toList)) postings
        DiskIndex.fromDocuments "index" (M.toList docIds) postings'

type SavedPostings p = M.Map Term [Posting p]

zipWithList :: Monad m => [i] -> Pipe a (i,a) m r
zipWithList = go
  where
    go (i:is) = do
        x <- await
        yield (i, x)
        go is

consumePostings :: Monad m
                => Producer ((DocumentId, DocumentName), TermPostings p) m ()
                -> m (M.Map DocumentId (DocumentName, DocumentLength), M.Map Term [Posting p])
consumePostings =
    foldProducer ((,) <$> docMeta
                      <*> lmap snd postings)
  where
    docMeta  = Foldl.generalize
               $ lmap (\((docId, docName), postings) -> M.singleton docId (docName, DocLength $ sum $ fmap length postings))
                      Foldl.mconcat
    postings = Foldl.generalize $ fmap (fmap toList) foldPostings

cleanHtml :: T.Text -> T.Text
cleanHtml content =
    let Clean.HtmlDocument {..} = Clean.clean content
    in T.L.toStrict $ docTitle <> "\n" <> docBody

