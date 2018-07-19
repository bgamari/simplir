module BTree.File
    ( BTreePath(..)
    , open
    , merge
    , fromOrdered
    , BTree.lookup
    ) where

import Control.Monad.IO.Class
import Control.Monad.Catch
import qualified BTree
import Pipes
import qualified Pipes.Prelude as P.P
import Data.Binary

newtype BTreePath k a = BTreePath {getBTreePath :: FilePath}

open :: BTreePath k a -> IO (BTree.LookupTree k a)
open (BTreePath path) =
    either error id <$> BTree.open path

merge :: (Ord k, Binary k, Binary a)
      => (a -> a -> a) -> BTreePath k a -> [BTreePath k a] -> IO ()
merge f (BTreePath outPath) trees = do
    trees' <- mapM open trees
    BTree.mergeTrees (\a b -> pure $ f a b) 64 outPath trees'
{-# INLINE merge #-}

fromOrdered :: (MonadIO m, MonadMask m, Binary k, Binary a)
            => BTree.Size -> BTreePath k a -> Producer (k, a) m () -> m ()
fromOrdered maxSize (BTreePath path) xs =
    BTree.fromOrderedToFile 64 maxSize path (xs >-> P.P.map (uncurry BTree.BLeaf))
{-# INLINE fromOrdered #-}
