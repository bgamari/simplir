{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

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
    , lookupPostings'
    , lookupDocument
    ) where

import Control.Concurrent.STM
import Control.Exception
import Data.Foldable
import Data.Semigroup
import Data.Word
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
import System.IO.Unsafe

import qualified SimplIR.EncodedList.Cbor as ELC
import qualified SimplIR.Encoded.Cbor as EC

newtype DiskIndexPath term termInfo doc p = DiskIndexPath { getDiskIndexPath :: FilePath }

docIndexPath :: DiskIndexPath term termInfo doc p -> FilePath
docIndexPath (DiskIndexPath p) = p </> "documents"

postingIndexPath :: DiskIndexPath term termInfo doc p -> FilePath
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

data DiskIndex (mode :: Mode) term termInfo doc p
    = DiskIndex { docIndex :: !K.Tree
                , postingIndex :: !K.Tree
                , nextDocId :: TVar DocId
                , indexPath :: DiskIndexPath term termInfo doc p
                , termLock :: TMVar () -- TODO bucket locks
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
--  * A term summary key has no further key suffix and contains a
--    'Serialise'-encoded @termInfo@
--  * Multiple term postings keys are suffixed with the first document ID of the
--    chunk. Each contains a @CborList (Posting p)@.

-- | Open an on-disk index.
--
-- The path should be the directory of a valid 'DiskIndex'
open :: forall term termInfo doc p mode. ()
     => DiskIndexPath term termInfo doc p -> IO (DiskIndex mode term termInfo doc p)
open indexPath = do
    docIndex <- K.openTree (docIndexPath indexPath) loggingOpts (K.Reader [K.NoLock, K.NoRepair])
    postingIndex <- K.openTree (postingIndexPath indexPath) loggingOpts (K.Reader [K.NoLock, K.NoRepair])
    Just nextDocId' <- K.get docIndex "next-docid"
    let Right (_, _, nextDocId') = B.decodeOrFail nextDocId'
    nextDocId <- newTVarIO $ B.decode nextDocId'
    termLock <- newTMVarIO ()
    return $ DiskIndex {..}

withIndex :: DiskIndexPath term termInfo doc p
          -> (DiskIndex mode term termInfo doc p -> IO a)
          -> IO a
withIndex indexPath = bracket (open indexPath) close

loggingOpts :: K.LoggingOptions
loggingOpts = K.defaultLoggingOptions

create :: forall term termInfo doc p. ()
       => FilePath -> IO (DiskIndexPath term termInfo doc p)
create dest = do
    let indexPath = DiskIndexPath dest
    docIndex <- K.makeTree (docIndexPath indexPath) loggingOpts treeOpts (K.Writer [K.Create] [K.TryLock])
    postingIndex <- K.makeTree (docIndexPath indexPath) loggingOpts treeOpts (K.Writer [K.Create] [K.TryLock])
    nextDocId <- newTVarIO $ DocId 1
    termLock <- newTMVarIO ()
    close $ DiskIndex {..}
    return indexPath
  where
    treeOpts = K.defaultTreeOptions

close :: DiskIndex mode term termInfo doc p -> IO ()
close DiskIndex{..} = do
    atomically $ takeTMVar termLock
    docId <- atomically $ readTVar nextDocId
    K.set docIndex "next-docid" (BSL.toStrict $ B.encode docId)
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

withTermLock :: DiskIndex 'ReadWriteMode term termInfo doc p -> IO a -> IO a
withTermLock DiskIndex{..} = bracket takeLock putLock . const
  where
    takeLock = atomically $ takeTMVar termLock
    putLock _ = atomically $ putTMVar termLock ()

addDocuments
    :: forall doc p term termInfo.
       (S.Serialise doc, S.Serialise p, S.Serialise term, Ord term, S.Serialise termInfo, Semigroup termInfo)
    => DiskIndex 'ReadWriteMode term termInfo doc p
    -> Foldl.FoldM IO (V.Vector (doc, M.Map term (termInfo, p))) Int
       -- ^ returns number of added documents
addDocuments idx@DiskIndex{..} = Foldl.FoldM step initial finish
  where
    treeOpts = K.defaultTreeOptions
    step :: Int -> V.Vector (doc, M.Map term (termInfo, p)) -> IO Int
    step count docs = do
        let !count' = count + V.length docs
        docId0 <- atomically $ do
            DocId docId <- readTVar nextDocId
            writeTVar nextDocId $ DocId $ docId + fromIntegral (V.length docs)
            return (DocId docId)

        K.setBulk docIndex
            [ (BSL.toStrict $ B.encode docId, BSL.toStrict $ S.serialise doc)
            | (docId, (doc, _termPostings)) <- zip [docId0..] $ V.toList docs
            ]
            notAtomic

        let inverted :: M.Map term (termInfo, Seq.Seq (Posting p))
            inverted = M.fromListWith (<>)
                [ (term, (termInfo, Seq.singleton (Posting docId posting)))
                | (docId, (_doc, termPostings)) <- zip [docId0..] $ V.toList docs
                , (term, (termInfo, posting)) <- M.assocs termPostings
                ]

        K.setBulk postingIndex
            [ ( BSL.toStrict $ S.serialise term
              , BSL.toStrict $ S.serialise $ ELC.fromList $ toList $ postings
              )
            | (term, (_, postings)) <- M.assocs inverted
            ]
            notAtomic

        withTermLock idx $ forM_ (M.assocs inverted) $ \(term, (termInfo, _)) -> do
            let key = termKey term
            mbTermInfo0 <- K.get postingIndex key
            case mbTermInfo0 of
              Just termInfo0 -> let Right termInfo0' = S.deserialiseOrFail $ BSL.fromStrict termInfo0
                                in K.replace postingIndex key $ BSL.toStrict $ S.serialise $ termInfo0' <> termInfo
              Nothing        -> K.set postingIndex key $ BSL.toStrict $ S.serialise termInfo

        return count'

    initial = return 0
    finish count = return count

-- | Lookup postings for a term.
lookupPostings
    :: forall term termInfo p doc.
       (S.Serialise termInfo, S.Serialise term, S.Serialise p, S.Serialise doc)
    => DiskIndex 'ReadMode term termInfo doc p
    -> term
    -> Maybe (termInfo, [(doc, p)])
lookupPostings idx term =
    fmap (fmap $ map f) $ lookupPostings' idx term
  where
    f (Posting docId p) = (lookupDocument idx docId, p)

-- | Lookup postings for a term.
lookupPostings'
    :: forall term termInfo p doc.
       (S.Serialise termInfo, S.Serialise term, S.Serialise p)
    => DiskIndex 'ReadMode term termInfo doc p
    -> term
    -> Maybe (termInfo, [Posting p])
-- It's unsafe to allow querying on Writeable indexes since term update is not
-- atomic.
lookupPostings' idx@DiskIndex{..} term = unsafePerformIO $ do
    cur <- K.cursor postingIndex
    K.curJumpKey cur termPrefix
    key <- K.curGetKey cur False
    if termPrefix `BS.isPrefixOf` key
      then do termInfo <- S.deserialise . BSL.fromStrict <$> K.curGetValue cur True
              postings <- unsafeInterleaveIO $ go cur
              return $ Just (termInfo, postings)
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
               => DiskIndex mode term termInfo doc p -> DocId -> doc
lookupDocument idx = unsafePerformIO . lookupDocumentIO idx

lookupDocumentIO :: S.Serialise doc
                 => DiskIndex mode term termInfo doc p -> DocId -> IO doc
lookupDocumentIO DiskIndex{..} doc = do
    Just bs <- K.get docIndex (BSL.toStrict $ B.encode doc)
    return $ S.deserialise $ BSL.fromStrict bs
