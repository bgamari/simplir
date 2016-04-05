{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Monad.Trans.Except
import Control.Monad.State.Strict hiding ((>=>))
import Data.Functor.Contravariant
import Data.Foldable
import Data.Profunctor
import Data.Monoid

import qualified Data.DList as DList
import           Data.DList (DList)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BS.L
import qualified Data.Map.Strict as M
import qualified Data.Map.Lazy as M.Lazy
import qualified Data.HashSet as HS
import qualified Data.Text as T
import qualified Data.Text.Lazy as T.L
import qualified Data.Vector.Unboxed as VU
import qualified Control.Foldl as Foldl

import Control.Monad.Morph
import           Pipes
import qualified Pipes.Prelude as P.P

import Data.Warc as Warc
import qualified Network.HTTP.Types as Http
import qualified Text.HTML.Clean as Clean

import Types
import Progress
import Tokenise
import WarcDocSource
import AccumPostings
import DataSource

{-
dsrc = S3Object { s3Bucket = "aws-publicdatasets"
                , s3Object = "common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"
                }
-}

dsrc = LocalFile "../0000tw-00.warc"
compression = Nothing

main :: IO ()
main = do
    compProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    expProg <- newProgressVar :: IO (ProgressVar (Sum Int))
    pollProgress compProg 1 $ \(Sum n) -> "Compressed "++show (realToFrac n / 1024 / 1024)
    pollProgress expProg 1 $ \(Sum n) -> "Expanded "++show (realToFrac n / 1024 / 1024)

    let normTerms :: Monoid p => M.Map Term p -> M.Map Term p
        normTerms = filterTerms . caseNorm
          where
            -- Avoid concatenating vectors unless necessary
            caseNorm = M.Lazy.mapKeysWith mappend toCaseFold
            filterTerms = M.filterWithKey (\k _ -> k `HS.member` takeTerms)
            takeTerms = HS.fromList [ "concert", "always", "musician", "beer", "watch", "table" ]

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
            -- >-> P.P.map (fmap $ M.toList . normTerms . M.fromList)
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> zipWithList [DocId 0..]
            >-> cat'                                          @(DocumentId, (DocumentName, [(Term, Position)]))
            >-> P.P.map (\(docId, (docName, postings)) ->
                          ((docId, docName),
                           toPostings docId
                           $ M.assocs
                           $ foldTokens accumPositions postings))
            >-> cat'                                          @((DocumentId, DocumentName), TermPostings (VU.Vector Position))

        liftIO $ print postings
        savePostings postings

zipWithList :: Monad m => [i] -> Pipe a (i,a) m r
zipWithList = go
  where
    go (i:is) = do
        x <- await
        yield (i, x)
        go is

cat' :: forall a m r. Monad m => Pipe a a m r
cat' = cat

savePostings :: MonadIO m => M.Map Term (DList (Posting p)) -> m ()
savePostings = undefined

foldProducer :: Monad m => Foldl.FoldM m a b -> Producer a m () -> m b
foldProducer (Foldl.FoldM step initial extract) =
    P.P.foldM step initial extract

consumePostings :: Monad m
                => Producer ((DocumentId, DocumentName), TermPostings p) m ()
                -> m (M.Map DocumentId DocumentName, M.Map Term (DList (Posting p)))
consumePostings =
    foldProducer ((,) <$> lmap fst docIds
                      <*> lmap snd postings)
  where
    postings = Foldl.generalize foldPostings
    docIds   = Foldl.generalize $ lmap (uncurry M.singleton) Foldl.mconcat

cleanHtml :: T.Text -> T.Text
cleanHtml content =
    let Clean.HtmlDocument {..} = Clean.clean content
    in T.L.toStrict $ docTitle <> "\n" <> docBody

