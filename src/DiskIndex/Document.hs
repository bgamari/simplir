{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-- | Document metadata index
module DiskIndex.Document where

import Data.Binary
import qualified Data.Map as M
import qualified Data.ByteString.Lazy as BSL

import Types

-- | TODO
newtype DocIndex meta = DocIndex (M.Map DocumentId meta)
                      deriving (Binary)

write :: Binary meta => FilePath -> M.Map DocumentId meta -> IO ()
write outFile docs = BSL.writeFile outFile $ encode docs

open :: Binary meta => FilePath -> IO (DocIndex meta)
open file = decode <$> BSL.readFile file

lookupDoc :: DocumentId -> DocIndex meta -> Maybe meta
lookupDoc docId (DocIndex idx) = M.lookup docId idx

size :: DocIndex meta -> Int
size (DocIndex idx) = M.size idx
