{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | GloVe: Global Vectors for Word Representation
--
-- See <https://nlp.stanford.edu/projects/glove/>
--
-- Reference: Jeffrey Pennington, Richard Socher, and Christopher D. Manning. 2014. /GloVe: Global Vectors for Word Representation/
module SimplIR.WordEmbedding.GloVe where

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

-- | Parse GloVe word embeddings from file.
readGlove :: FilePath -> IO SomeWordEmbedding
readGlove path = inCompact (parseGlove' <$> TL.readFile path)

-- | Parse GloVe word embeddings.
parseGlove' :: TL.Text -> SomeWordEmbedding
parseGlove' contents =
    case someNatVal $ fromIntegral dim of
      Just (SomeNat (Proxy :: Proxy n)) ->
          let parse :: KnownNat n => TL.Text -> HM.HashMap T.Text (WordVec n)
              parse line =
                case TL.words line of
                  []     -> mempty
                  w : ws -> HM.singleton (TL.toStrict w)
                                         (WordVec $ VI.fromList dimRange $ map parseFloat ws)

              dimRange :: (EmbeddingDim n, EmbeddingDim n)
              dimRange = (minBound, maxBound)

              parseFloat :: TL.Text -> Float
              parseFloat = either err (realToFrac . fst) . TR.double . TL.toStrict
                where err msg = error $ "GloveEmbedding.parseGlove: parse error: "++msg
          in SomeWordEmbedding $
              let vecs :: KnownNat n => WordEmbedding n
                  vecs = mconcat $ map parse $ TL.lines contents
              in vecs
      Nothing -> error "parseGlove': impossible"
  where
    !dim | x:_ <- TL.lines contents = length (TL.words x) - 1
         | otherwise = error "GloveEmbedding.parseGlove': Empty embeddings"
