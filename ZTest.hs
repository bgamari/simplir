import Control.Monad (unless)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Codec.Compression.Zlib    as ZC
import qualified Codec.Zlib                as Z

main :: IO ()
main = do
    bs <- LBS.getContents
    printDecompress bs

printDecompress :: LBS.ByteString -> IO ()
printDecompress = start . LBS.toChunks
  where
    start chunks = do
        let wbits = ZC.WindowBits 31
        inf <- Z.initInflate wbits
        go inf chunks

    go inf [] = do
        bs <- Z.finishInflate inf
        BS.putStr bs

    go inf (bs:chunks) = do
        popper <- Z.feedInflate' inf bs
        r <- fromPopper popper
        case r of
            Just leftover -> do
                d <- Z.finishInflate inf
                BS.putStr d
                print (BS.length leftover)
                start (leftover:chunks)
            Nothing       -> go inf chunks

fromPopper :: Z.Popper' -> IO (Maybe BS.ByteString)
fromPopper pop = loop where
    loop = do
      mbs <- pop
      case mbs of
         Z.NothingYet -> return Nothing
         Z.Inflated bs -> putStr "POP" >> BS.putStr bs >> loop
         Z.StreamEnded bs -> return $ Just bs
