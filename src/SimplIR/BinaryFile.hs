module SimplIR.BinaryFile
    ( BinaryFile(..)
    , read
    , write
    , fold
    , mconcat
    ) where

import Data.Foldable (toList)
import Prelude hiding (read, mconcat)

import Data.Binary
import qualified Data.ByteString.Lazy as BS.L

-- | Represents a path to a local file containing an @binary@-encoded value.
newtype BinaryFile a = BinaryFile FilePath

read :: Binary a => BinaryFile a -> IO a
read (BinaryFile path) = decode <$> BS.L.readFile path

write :: Binary a => BinaryFile a -> a -> IO ()
write (BinaryFile path) = BS.L.writeFile path . encode

-- | Fold over the values in a set of 'BinaryFile's.
fold :: (Binary b, Foldable f)
      => (a -> b -> a) -> a -> f (BinaryFile b) -> IO a
fold f z = go z . toList
  where
    go acc [] = return acc
    go acc (x:xs) = do
        y <- read x
        go (f acc y) xs

mconcat :: (Binary a, Monoid a, Foldable f)
        => f (BinaryFile a) -> IO a
mconcat = foldMap read
