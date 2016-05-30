{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Data.Bifunctor
import Data.Char
import Data.Profunctor
import Data.Foldable

import Data.Binary
import qualified Data.Set as S
import qualified Data.ByteString.Short as BS.S
import qualified Data.ByteString.Lazy as BS.L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.IO as T.IO
import qualified Data.Text.Encoding as T.E
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import           Data.Vector.Algorithms.Intro (sort)
import qualified Control.Foldl as Foldl
import           Control.Foldl (FoldM, Fold)
import           Control.Monad.Trans.Except

import           Pipes
import           Pipes.Safe
import qualified Pipes.Prelude as P.P
import qualified Pipes.Text.Encoding as P.T

import Options.Applicative
import System.FilePath
import qualified BTree

import qualified Data.SmallUtf8 as Utf8
import Utils
import Types
import Term
import Tokenise
import SimplIR.TREC as TREC
import AccumPostings
import DataSource
import DiskIndex

opts :: Parser [DataLocation]
opts = some $ argument (LocalFile <$> str) (metavar "FILE" <> help "TREC input file")

compression = Just GZip

main :: IO ()
main = do
    dsrcs <- execParser $ info (helper <*> opts) mempty
    stopwords <- S.fromList . T.lines <$> T.IO.readFile "inquery-stopwordlist"

    let normTerms :: [(T.Text, p)] -> [(Term, p)]
        normTerms = map (first Term.fromText) . filterTerms . caseNorm
          where
            filterTerms = filter ((>2) . T.length . fst) . filter (not . (`S.member` stopwords) . fst)
            caseNorm = map (first $ T.filter isAlpha . T.toCaseFold)

    let docs :: Producer TREC.Document (SafeT IO) ()
        docs =
            mapM_ (trecDocuments' . P.T.decodeUtf8 . decompress compression . produce) dsrcs

    let chunkIndexes :: Producer (FragmentIndex (VU.Vector Position)) (SafeT IO) ()
        chunkIndexes =
                consumePostings 10000 (TermFreq . VU.length)
             $  docs
            >-> cat'                                          @TREC.Document
            >-> P.P.map (\d -> (DocName $ Utf8.fromText $ TREC.docNo d, TREC.docText d))
            >-> cat'                                          @(DocumentName, T.Text)
            >-> P.P.map (fmap tokeniseWithPositions)
            >-> cat'                                          @(DocumentName, [(T.Text, Position)])
            >-> P.P.map (fmap normTerms)
            >-> cat'                                          @(DocumentName, [(Term, Position)])
            >-> P.P.map (\(docName, terms) -> (docName, DocLength $ length terms, terms))
            >-> cat'                                          @(DocumentName, DocumentLength, [(Term, Position)])
            >-> zipWithList [DocId 0..]
            >-> cat'                                          @(DocumentId, (DocumentName, DocumentLength, [(Term, Position)]))
            >-> P.P.map (\(docId, (docName, docLen, postings)) ->
                          ((docId, docName, docLen)
                          , toPostings docId
                            $ M.assocs
                            $ foldTokens accumPositions postings
                          ))
            >-> cat'                                          @((DocumentId, DocumentName, DocumentLength), TermPostings (VU.Vector Position))

    chunks <- runSafeT $ P.P.toListM $ for (chunkIndexes >-> zipWithList [0..]) $ \(n, (docIds, postings, termFreqs, collLength)) -> do
        liftIO $ print (n, M.size docIds)
        let indexPath = "index-"++show n
        liftIO $ DiskIndex.fromDocuments indexPath (M.toList docIds) (fmap V.toList postings)
        liftIO $ BTree.fromOrderedToFile 32 (fromIntegral $ M.size termFreqs)
                                         (indexPath </> "term-freqs")
                                         (each $ map (uncurry BTree.BLeaf) $ M.toAscList termFreqs)
        liftIO $ BS.L.writeFile (indexPath </> "coll-length") $ encode collLength
        yield indexPath

    chunkIdxs <- mapM DiskIndex.open chunks
              :: IO [DiskIndex (DocumentName, DocumentLength) (VU.Vector Position)]
    putStrLn $ "Merging "++show (length chunkIdxs)++" chunks"
    DiskIndex.merge "index" chunkIdxs

    collLengths <- mapM (\indexPath -> decode <$> BS.L.readFile (indexPath </> "coll-length")) $ chunks
        :: IO [Int]
    BS.L.writeFile ("index" </> "coll-length") $ encode (sum collLengths)

    Right trees <- runExceptT $ mapM (\indexPath -> ExceptT $ BTree.open (indexPath </> "term-freqs")) chunks
        :: IO (Either String [BTree.LookupTree Term TermFrequency])
    BTree.mergeTrees (\x y -> pure $ x <> y) 32 ("index" </> "term-freqs") trees

type SavedPostings p = M.Map Term (V.Vector (Posting p))
type FragmentIndex p =
    ( M.Map DocumentId (DocumentName, DocumentLength)
    , SavedPostings p
    , M.Map Term TermFrequency
    , CollectionLength)

type CollectionLength = Int

zipWithList :: Monad m => [i] -> Pipe a (i,a) m r
zipWithList = go
  where
    go []     = error "zipWithList: Reached end of list"
    go (i:is) = do
        x <- await
        yield (i, x)
        go is

foldChunks :: Monad m => Int -> FoldM m a b -> Producer a m () -> Producer b m ()
foldChunks chunkSize (Foldl.FoldM step initial extract) = start
  where
    start prod = do
        acc <- lift initial
        go chunkSize acc prod

    go 0 acc prod = do
        lift (extract acc) >>= yield
        start prod
    go n acc prod = do
        mx <- lift $ next prod
        case mx of
          Right (x, prod') -> do
              acc' <- lift $ step acc x
              go (n-1 :: Int) acc' prod'
          Left r -> lift (extract acc) >>= yield

consumePostings :: (Monad m, Ord p)
                => Int
                -> (p -> TermFrequency)
                -> Producer ((DocumentId, DocumentName, DocumentLength), TermPostings p) m ()
                -> Producer (FragmentIndex p) m ()
consumePostings chunkSize getTermFreq =
    foldChunks chunkSize (Foldl.generalize $ consumePostings' getTermFreq)

consumePostings' :: forall p. (Ord p)
                 => (p -> TermFrequency)
                 -> Fold ((DocumentId, DocumentName, DocumentLength), TermPostings p)
                         (FragmentIndex p)
consumePostings' getTermFreq =
    (,,,)
      <$> docMeta
      <*> lmap snd postings
      <*> lmap snd termFreqs
      <*> lmap fst collLength
  where
    docMeta  = lmap (\((docId, docName, docLen), postings) -> M.singleton docId (docName, docLen))
                      Foldl.mconcat
    postings = fmap (M.map $ VG.modify sort . fromFoldable) foldPostings

    termFreqs :: Fold (TermPostings p) (M.Map Term TermFrequency)
    termFreqs = Foldl.handles traverse
                $ lmap (\(term, ps) -> M.singleton term (getTermFreq $ postingBody ps))
                $ Foldl.mconcat

    collLength = lmap (\(a,b, DocLength c) -> c) Foldl.sum

fromFoldable :: (Foldable f, VG.Vector v a)
             => f a -> v a
fromFoldable xs = VG.fromListN (length xs) (toList xs)
