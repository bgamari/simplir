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
import           Pipes.Safe
import qualified Pipes.Prelude as P.P

import Data.Warc as Warc
import qualified SimplIR.HTML.Clean as Clean

import SimplIR.Utils
import SimplIR.Types
import SimplIR.Term as Term
import Progress
import SimplIR.Tokenise
import SimplIR.WarcDocSource
import AccumPostings
import SimplIR.DataSource
import SimplIR.DataSource.Compression
import SimplIR.DiskIndex as DiskIndex

--dsrc = S3Object { s3Bucket = "aws-publicdatasets"
--                , s3Object = "common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"
--                }
--dsrc = LocalFile "../0000tw-00.warc"
Just dsrc = parseDataSource localFile "data/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"

main :: IO ()
main = do
    compProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    expProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    pollProgress compProg 1 $ \(Sum n) -> "Compressed "++show (realToFrac n / 1024 / 1024)
    pollProgress expProg 1 $ \(Sum n) -> "Expanded "++show (realToFrac n / 1024 / 1024)

    takeTerms <- HS.fromList . map (Term.fromText . T.toCaseFold) . T.words <$> TIO.readFile "terms"
    let normTerms :: [(T.Text, p)] -> [(Term, p)]
        normTerms = filterTerms . map (first Term.fromText) . caseNorm
          where
            caseNorm = map (first $ T.toCaseFold)
            filterTerms = filter (\(k,_) -> k `HS.member` takeTerms)

    runSafeT $ do
        let warc :: Warc.Warc (SafeT IO) ()
            warc = Warc.parseWarc
                   $ decompressed (runDataSource dsrc >-> progressPipe compProg (Sum . BS.length))
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
            >-> cat'                                          @(DocumentName, [(T.Text, Position)])
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
        liftIO $ DiskIndex.fromDocuments "index" (M.toList docIds) postings'

type SavedPostings p = M.Map Term [Posting p]

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

