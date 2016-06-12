module SimplIR.BinaryFile where

import Data.Monoid
import Data.Binary
import qualified Data.ByteString.Lazy as BS.L

newtype BinaryFile a = BinaryFile FilePath

read :: Binary a => BinaryFile a -> IO a
read (BinaryFile path) = decode <$> BS.L.readFile path

write :: Binary a => BinaryFile a -> a -> IO ()
write (BinaryFile path) = BS.L.writeFile path . encode

fold :: Foldable f => (a -> b -> a) -> a -> f (BinaryFile b) -> IO b
fold f z = go z . toList
  where
    go acc [] = return acc
    go acc (x:xs) = do
        y <- read x
        go (f acc y) xs

mconcat :: Monoid a => [BinaryFile a] -> IO a
mconcat = foldMap read
