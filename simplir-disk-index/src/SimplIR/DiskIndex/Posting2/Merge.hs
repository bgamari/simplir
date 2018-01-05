{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module SimplIR.DiskIndex.Posting2.Merge where

import Data.Function (on)
import Data.Foldable (foldl', toList)
import Data.Maybe
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.Heap as H
import qualified Data.Vector as V

import Codec.Serialise (Serialise)
import SimplIR.Types
import SimplIR.Term
import qualified SimplIR.EncodedList.Cbor as ELC
import qualified SimplIR.Encoded.Cbor as E
import qualified SimplIR.DiskIndex.Posting2.Internal as PIdx
import SimplIR.DiskIndex.Posting2.PostingList

merge :: (Ord term, Serialise term, Serialise p)
      => FilePath
      -> [(DocIdDelta, PIdx.PostingIndexPath term p)]
      -> IO (PIdx.PostingIndexPath term p)
merge outPath indexPaths = do
    indexes <- mapM (traverse PIdx.walkTermPostings) indexPaths
    PIdx.fromChunks outPath $ interleavePostings indexes

data S term p = S { sTerm       :: !term
                  , sPostings   :: !(ELC.EncodedList (PostingsChunk p))
                  , sDocIdDelta :: !DocIdDelta
                  , sRest       :: [PIdx.TermPostings term p]
                  }
              deriving (Show)

instance Eq term => Eq (S term p) where
    (==) = (==) `on` sTerm

instance Ord term => Ord (S term p) where
    compare = compare `on` sTerm

adjustedPostings :: Serialise p
                 => S term p -> ELC.EncodedList (PostingsChunk p)
adjustedPostings s =
    ELC.map (applyDocIdDeltaToChunk (sDocIdDelta s)) (sPostings s)

interleavePostings :: forall term p. (Ord term, Serialise p)
                   => [(DocIdDelta, [PIdx.TermPostings term p])]
                   -> [PIdx.TermPostings term p]
interleavePostings = go . H.fromList . mapMaybe (uncurry initial)
  where
    initial :: DocIdDelta -> [PIdx.TermPostings term p]
            -> Maybe (S term p)
    initial _delta [] = Nothing
    initial delta (PIdx.TermPostings term ps : rest) =
        Just $ S term ps delta rest

    go :: H.Heap (S term p)
       -> [PIdx.TermPostings term p]
    go h
      | Just (xs@(x :| _), h') <- takeMins h
      = let tp = PIdx.TermPostings (sTerm x) (foldMap adjustedPostings xs)
            h'' = foldl' (flip H.insert) h'
                  [ s'
                  | s <- toList xs
                  , Just s' <- pure $ initial (sDocIdDelta s) (sRest s)
                  ]
        in tp : go h''
    go _ = []


takeMins :: Ord a => H.Heap a -> Maybe (NonEmpty a, H.Heap a)
takeMins h
  | Just (x,xs) <- H.uncons h
  = let (a,b) = H.span (==x) xs
    in Just (x :| toList a, b)
  | otherwise = Nothing

-- TODO: Write proper property
test :: [(DocIdDelta, [PIdx.TermPostings Term ()])]
test =
    [ (DocIdDelta 0,
       [ term "cat"    [chunk [0,4],   chunk [11, 15, 19]]
       , term "turtle" [chunk [0,1,4], chunk [10, 15, 18]]
       ])
    , (DocIdDelta 100,
       [ term "dog"    [chunk [0,4],   chunk [11, 18, 19]]
       , term "turtle" [chunk [0,1,4], chunk [10, 15, 18]]
       ])
    ]
  where
    term :: Term -> [PostingsChunk ()] -> PIdx.TermPostings Term ()
    term t chunks =
        PIdx.TermPostings t (ELC.fromList chunks)

    chunk :: [Int] -> PostingsChunk ()
    chunk docIds' =
        Chunk docId0
              (E.encode $ V.fromList $ map (\x->(docId0 `docIdDelta` x, ())) docIds)
      where
        docId0 = head docIds
        docIds = map DocId docIds'
