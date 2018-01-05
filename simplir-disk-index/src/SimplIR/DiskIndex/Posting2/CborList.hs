{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module SimplIR.DiskIndex.Posting2.CborList
    ( CborListPath(..)
    , Offset
    , DeserialiseFailure(..)

      -- * Simple walking
      -- | These use typical @read@ IO and should be used where one-pass
      -- traversal is needed.
    , walk
    , walkWithOffsets

      -- * Mapping
      -- | These use @mmap@ and should be used where random-access is needed.
    , CborList
    , open
    , index
    , toList
    , toListWithOffsets

      -- * Creation
    , fromList
    , folded
    ) where

import Control.Monad.IO.Class
import Pipes.Safe
import qualified Control.Foldl as Foldl
import Control.Exception (Exception, throw)
import Data.Foldable hiding (toList)
import qualified Codec.Serialise as CBOR
import qualified Codec.CBOR.Read as CBOR
import qualified Codec.CBOR.Write as CBOR
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import System.IO.MMap
import System.IO
import Prelude hiding (lookup)

newtype Offset a = Offset Int

data CborList a = CborList BS.ByteString String

newtype CborListPath a = CborListPath FilePath
                       deriving (Show)

deserialiseListWithOffsets :: forall a. (CBOR.Serialise a)
                           => String -> BSL.ByteString -> [(Offset a, a)]
deserialiseListWithOffsets desc = go 0
  where
    go :: Int -- ^ current offset
       -> BSL.ByteString
       -> [(Offset a, a)]
    go !offset !bs
      | BSL.null bs = []
      | otherwise   =
          case CBOR.deserialiseFromBytesWithSize CBOR.decode bs of
            Left err             -> throw $ DeserialiseFailure "CborList.toListWithOffsets" desc err
            Right (rest, sz, x)  -> (Offset offset, x) : go (offset + fromIntegral sz) rest

deserialiseList :: (CBOR.Serialise a) => String -> BSL.ByteString -> [a]
deserialiseList desc =
    go
  where
    go bs
      | BSL.null bs = []
      | otherwise   =
            case CBOR.deserialiseFromBytes CBOR.decode bs of
              Left err        -> throw $ DeserialiseFailure "CborList.decodeRawCborList" desc err
              Right (rest, x) -> x : go rest

toListWithOffsets :: forall a. CBOR.Serialise a
                  => CborList a -> [(Offset a, a)]
toListWithOffsets (CborList bs desc) = deserialiseListWithOffsets desc $ BSL.fromStrict bs

toList :: (CBOR.Serialise a) => CborList a -> [a]
toList (CborList bs desc) = deserialiseList desc $ BSL.fromStrict bs

-- | Simply walk a list once
walk :: (CBOR.Serialise a) => CborListPath a -> IO [a]
walk (CborListPath fname) = deserialiseList fname <$> BSL.readFile fname

walkWithOffsets :: (CBOR.Serialise a) => CborListPath a -> IO [(Offset a, a)]
walkWithOffsets (CborListPath fname) = deserialiseListWithOffsets fname <$> BSL.readFile fname

open :: CborListPath a -> IO (CborList a)
open (CborListPath fname) = do
    cbor <- mmapFileByteString fname Nothing
    return $ CborList cbor fname

index :: CBOR.Serialise a => CborList a -> Offset a -> a
index (CborList cbor desc) (Offset off) =
    either uhOh snd
    $ CBOR.deserialiseFromBytes CBOR.decode
    $ BSL.fromStrict $ BS.drop off cbor
  where
    uhOh = throw . DeserialiseFailure "CborList.index" desc

fromList :: CBOR.Serialise a => FilePath -> [a] -> IO (CborListPath a)
fromList path xs = do
    BSL.writeFile path $ CBOR.toLazyByteString $ foldMap CBOR.encode xs
    return $ CborListPath path

folded :: (MonadSafe m, CBOR.Serialise a) => FilePath -> Foldl.FoldM m a (CborListPath a)
folded path = Foldl.FoldM step initial extract
  where
    initial = do
        h <- liftIO $ openFile path WriteMode
        key <- register $ liftIO $ hClose h
        return (h, key)
    extract (_, key) = release key >> return (CborListPath path)
    step acc@(h, _) x = do
        liftIO $ BSL.hPutStr h $ CBOR.toLazyByteString $ CBOR.encode x
        return acc

data DeserialiseFailure = DeserialiseFailure String FilePath CBOR.DeserialiseFailure
                        deriving (Show)
instance Exception DeserialiseFailure

