{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SimplIR.WordEmbedding.Parse
    ( readWordEmbedding
      -- * GloVe
      -- | GloVe: Global Vectors for Word Representation
      --
      -- See <https://nlp.stanford.edu/projects/glove/>
      --
      -- Reference: Jeffrey Pennington, Richard Socher, and Christopher D.
      -- Manning. 2014. /GloVe: Global Vectors for Word Representation/
    , readGlove
    , parseGlove
      -- * word2vec
    , readWord2vec
    , parseWord2vec
    ) where

import GHC.TypeLits
import Data.Proxy

import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
import qualified Data.Text.Read as TR
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector.Indexed as VI

import SimplIR.WordEmbedding
import SimplIR.Utils.Compact

-- | Tries to guess iput format.
readWordEmbedding :: FilePath -> IO SomeWordEmbedding
readWordEmbedding path = do
    contents <- TL.readFile path
    case TL.words $ head $ TL.lines contents of
        [_,_] -> return $ parseWord2vec contents
        _     -> return $ parseGlove contents

-- | Parse GloVe word embeddings from file.
readGlove :: FilePath -> IO SomeWordEmbedding
readGlove path = inCompactM (parseGlove <$> TL.readFile path)

readWord2vec :: FilePath -> IO SomeWordEmbedding
readWord2vec path = inCompactM (parseWord2vec <$> TL.readFile path)

parseWord2vec :: TL.Text -> SomeWordEmbedding
parseWord2vec contents =
    case someNatVal $ fromIntegral dim of
      Just (SomeNat (Proxy :: Proxy n)) ->
        let vecs :: WordEmbedding n
            vecs = parseVectors $ TL.tail $ TL.dropWhile (/= '\n') contents
        in SomeWordEmbedding vecs
      Nothing -> error "parseGlove: impossible"
  where
    !dim
      | l:_ <- TL.lines contents
      , nVecs : dim : [] <- TL.words l
      , Right (n,_) <- TR.decimal $ TL.toStrict dim
      = n
      | otherwise = error "GloveEmbedding.parseWord2vec: Invalid header"


parseGlove :: TL.Text -> SomeWordEmbedding
parseGlove contents =
    case someNatVal $ fromIntegral dim of
      Just (SomeNat (Proxy :: Proxy n)) ->
        let vecs :: WordEmbedding n
            vecs = parseVectors contents
        in SomeWordEmbedding vecs
      Nothing -> error "parseGlove: impossible"
  where
    !dim | x:_ <- TL.lines contents = length (TL.words x) - 1
         | otherwise = error "GloveEmbedding.parseGlove: Empty embeddings"


-- | Parse GloVe word embeddings.
parseVectors :: forall n. KnownNat n => TL.Text -> WordEmbedding n
parseVectors contents =
    let parse :: KnownNat n => Int -> TL.Text -> HM.HashMap T.Text (WordVec n)
        parse lineNo line =
          case TL.split isDelim line of
            []     -> mempty
            w : ws -> HM.singleton (TL.toStrict w)
                                   (WordVec $ VI.fromList dimRange $ map (parseFloat lineNo) ws)

        isDelim ' '  = True
        isDelim '\t' = True
        isDelim _    = False

        dimRange :: (EmbeddingDim n, EmbeddingDim n)
        dimRange = (minBound, maxBound)

        parseFloat :: Int -> TL.Text -> Float
        parseFloat lineNo txt = either err (realToFrac . fst) $ TR.double $ TL.toStrict txt
          where err msg =
                    error $ concat [ "GloveEmbedding.parseVectors: parse error (line ", show lineNo, "): ", msg, ": ", show txt ]
    in mconcat $ zipWith parse [1..] $ TL.lines contents
