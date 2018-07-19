{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -fprof-auto #-}

module SimplIR.KyotoIndex
    ( DiskIndexPath(..)
    , DiskIndex
    , indexPath
      -- * Creation
    , create
      -- * Adding documents
    , addDocuments
      -- * Opening
    , withIndex
    , Mode(..)
      -- * Queries
    , lookupPostings
    , Posting(..)
    , lookupPostings'
    , lookupDocument
    ) where

import Debug.Trace
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Data.Bits
import Data.Foldable
import Data.Hashable
import Data.Proxy
import Data.Semigroup
import Data.Word
import qualified Data.Array as A
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Sequence as Seq
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import qualified Control.Foldl as Foldl
import qualified Database.KyotoCabinet.DB.Tree as K
import qualified Database.KyotoCabinet.Operations as K
import qualified Codec.Serialise as S
import System.FilePath
import System.Directory
import System.IO.Unsafe
import System.CPUTime

import qualified SimplIR.EncodedList.Cbor as ELC
import qualified SimplIR.Encoded.Cbor as EC

newtype DiskIndexPath term doc p = DiskIndexPath { getDiskIndexPath :: FilePath }

docIndexPath :: DiskIndexPath term doc p -> FilePath
docIndexPath (DiskIndexPath p) = p </> "documents"

postingIndexPath :: DiskIndexPath term doc p -> FilePath
postingIndexPath (DiskIndexPath p) = p </> "postings"

notAtomic :: Bool
notAtomic = False

data Mode = ReadMode | ReadWriteMode

newtype DocId = DocId Word32
              deriving (Show, Enum, Eq, Ord, B.Binary, S.Serialise)

data Posting p = Posting !DocId p

instance S.Serialise p => S.Serialise (Posting p) where
    encode (Posting a b) = S.encode a <> S.encode (EC.encode b)
    decode = do
        a <- S.decode
        b <- EC.decode <$> S.decode
        return (Posting a b)

data DiskIndex (mode :: Mode) term doc p
    = DiskIndex { docIndex :: !K.Tree
                , postingIndex :: !K.Tree
                , nextDocId :: !(MVar DocId)
                , indexPath :: DiskIndexPath term doc p
                , writable :: Bool -- TODO break out a new type
                }

-- $index-structure
--
-- The index consists of two B-tree databases: a posting index and a document index.
-- The document index contains:
--
--   * A @"next-docid"@ key containing a 'Binary'-serialised 'DocId'. This is
--     the next available document ID in the index.
--
--   * A value for each document (serialised with 'Serialise') keyed on its
--     document ID (serialised with 'Binary').
--
-- The posting index contains a set of entries for each term. Each begins with a prefix
-- of the length of the term's 'Serialise'-encoded representation, followed by
-- the representation itself:
--
--  * Multiple term postings keys are suffixed with the first document ID of the
--    chunk. Each contains a @CborList (Posting p)@.

class IsWritable (mode :: Mode) where
    isWritable :: Proxy mode -> Bool

instance IsWritable ReadWriteMode where isWritable _ = True
instance IsWritable ReadMode where isWritable _ = False

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: forall term doc p mode.
        (IsWritable mode, S.Serialise term, Hashable term)
     => DiskIndexPath term doc p -> IO (DiskIndex mode term doc p)
open indexPath = do
    let writable = isWritable $ Proxy @mode
        access
          | writable  = K.Writer [] [K.TryLock]
          | otherwise = K.Reader [K.NoLock, K.NoRepair]
    docIndex <- K.openTree (docIndexPath indexPath) loggingOpts access
    postingIndex <- K.openTree (postingIndexPath indexPath) loggingOpts access
    Just nextDocId <- K.get docIndex "next-docid"
    let Right (_, _, nextDocId') = B.decodeOrFail $ BSL.fromStrict nextDocId
    --when writable $ async $ forever $ do
    --    threadDelay (30*1000*1000)
    --    K.synchronize postingIndex False
    --    K.synchronize docIndex False

    nextDocId <- newMVar nextDocId'
    return DiskIndex {..}

withIndex :: (MonadMask m, MonadIO m, IsWritable mode, S.Serialise term, Hashable term)
          => DiskIndexPath term doc p
          -> (DiskIndex mode term doc p -> m a)
          -> m a
withIndex indexPath = bracket (liftIO $ open indexPath) (liftIO . close)

loggingOpts :: K.LoggingOptions
loggingOpts = K.defaultLoggingOptions

treeOpts :: K.TreeOptions
treeOpts = K.defaultTreeOptions
    { K.buckets = Just (64 * 1024 * 1024) -- TODO: should be 10% record count
    , K.pageCacheSize = Just (1 * 1024 * 1024 * 1024)
    }

create :: forall term doc p. ()
       => FilePath -> IO (DiskIndexPath term doc p)
create dest = do
    let indexPath = DiskIndexPath dest
    createDirectory dest
    docIndex <- K.makeTree (docIndexPath indexPath) loggingOpts treeOpts (K.Writer [K.Create] [K.TryLock])
    postingIndex <- K.makeTree (postingIndexPath indexPath) loggingOpts treeOpts (K.Writer [K.Create] [K.TryLock])
    nextDocId <- newMVar $ DocId 1
    let writable = True
    close $ DiskIndex {..}
    return indexPath
  where
    treeOpts = K.defaultTreeOptions

close :: DiskIndex mode term doc p -> IO ()
close DiskIndex{..} = do
    docId <- readMVar nextDocId
    when writable $ K.set docIndex "next-docid" (BSL.toStrict $ B.encode docId)
    K.close docIndex
    K.close postingIndex

postingsKey :: S.Serialise term => term -> DocId -> BS.ByteString
postingsKey term docId = BSL.toStrict $ B.runPut $
    B.putWord16le (fromIntegral $ BSL.length t') <> B.putLazyByteString t' <> B.put docId
  where t' = S.serialise term

termKey :: S.Serialise term => term -> BS.ByteString
termKey term = BSL.toStrict $ B.runPut $
    B.putWord16le (fromIntegral $ BSL.length t') <> B.putLazyByteString t'
  where t' = S.serialise term

postingsKeyDocId :: BS.ByteString -> DocId
postingsKeyDocId = B.runGet parse . BSL.fromStrict
  where
    parse = do
        len <- B.getWord16le
        B.skip $ fromIntegral len
        B.get

documentKey :: DocId -> BS.ByteString
documentKey = BSL.toStrict . B.encode

addDocuments
    :: forall doc p term m.
       (S.Serialise doc, S.Serialise p,
        S.Serialise term, Ord term, Hashable term,
        MonadIO m)
    => DiskIndex 'ReadWriteMode term doc p
    -> Foldl.FoldM m (V.Vector (doc, M.Map term p)) Int
       -- ^ returns number of added documents
addDocuments idx@DiskIndex{..} = Foldl.FoldM step initial finish
  where
    step :: Int -> V.Vector (doc, M.Map term p) -> m Int
    step !count docs = liftIO $ do
        let !nDocs = V.length docs
            !count' = count + nDocs
        docId0 <- modifyMVar nextDocId $ \(DocId docId0) ->
          let !docId' = DocId (docId0 + fromIntegral nDocs)
          in return (docId', DocId docId0)
        print (docId0, nDocs, getSum $ foldMap (Sum . M.size . snd) docs)

        K.setBulk docIndex
            [ (BSL.toStrict $ B.encode docId, BSL.toStrict $ S.serialise doc)
            | (docId, (doc, _termPostings)) <- zip [docId0..] $ V.toList docs
            ]
            notAtomic
        print (docId0, "inserted docs")

        let inverted :: M.Map term (Seq.Seq (Posting p))
            inverted = M.fromListWith (<>)
                [ (term, (Seq.singleton (Posting docId posting)))
                | (docId, (_doc, termPostings)) <- zip [docId0..] $ V.toList docs
                , (term, posting) <- M.assocs termPostings
                ]

        K.setBulk postingIndex
            [ ( postingsKey term docId0
              , BSL.toStrict $ S.serialise $ ELC.fromList $ toList $ postings
              )
            | (term, postings) <- M.assocs inverted
            ]
            notAtomic
        print (docId0, "inserted postings")

        return $! count'

    initial = return 0
    finish count = return count

-- | Lookup postings for a term.
lookupPostings
    :: forall term p doc.
       (S.Serialise term, S.Serialise p, S.Serialise doc)
    => DiskIndex 'ReadMode term doc p
    -> term
    -> Maybe [(doc, p)]
lookupPostings idx term =
    fmap (map f) $ lookupPostings' idx term
  where
    f (Posting docId p) = (lookupDocument idx docId, p)

-- | Lookup postings for a term.
lookupPostings'
    :: forall term p doc.
       (S.Serialise term, S.Serialise p)
    => DiskIndex 'ReadMode term doc p
    -> term
    -> Maybe [Posting p]
-- It's unsafe to allow querying on Writeable indexes since term update is not
-- atomic.
lookupPostings' idx@DiskIndex{..} term = unsafePerformIO $ do
    cur <- K.cursor postingIndex
    K.curJumpKey cur termPrefix
    key <- K.curGetKey cur False
    if termPrefix `BS.isPrefixOf` key
      then do postings <- unsafeInterleaveIO $ go cur
              return $ Just postings
      else return Nothing
  where
    !termPrefix = termKey term

    go :: K.Cursor -> IO [Posting p]
    go cur = do
        (k,v) <- K.curGet cur True
        if termPrefix `BS.isPrefixOf` k
          then do let p = ELC.toList $ S.deserialise $ BSL.fromStrict v
                  rest <- unsafeInterleaveIO $ go cur
                  return (p ++ rest)
          else return []

lookupDocument :: S.Serialise doc
               => DiskIndex mode term doc p -> DocId -> doc
lookupDocument idx = unsafePerformIO . lookupDocumentIO idx

lookupDocumentIO :: S.Serialise doc
                 => DiskIndex mode term doc p -> DocId -> IO doc
lookupDocumentIO DiskIndex{..} doc = do
    Just bs <- K.get docIndex (BSL.toStrict $ B.encode doc)
    return $ S.deserialise $ BSL.fromStrict bs
