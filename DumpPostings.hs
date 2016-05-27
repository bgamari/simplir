{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Monad
import qualified DiskIndex
import qualified DiskIndex.Posting as PostingIdx
import Types
import Options.Applicative

args :: Parser (FilePath, [String])
args =
    (,)
      <$> option str (long "index" <> short 'i' <> help "index directory")
      <*> many (option str (long "term" <> short 't' <> help "term"))

main :: IO ()
main = do
    (index, terms) <- execParser $ info (helper <*> args) mempty
    idx <- DiskIndex.open index :: IO (DiskIndex.DiskIndex (DocumentName, DocumentLength) [Position])

    let toDocName :: Show a => Posting a -> String
        toDocName = maybe "none" show . flip DiskIndex.lookupDoc idx . postingDocId
    if null terms
      then
        forM_ (PostingIdx.walk $ DiskIndex.tfIdx idx) $ \(term, postings) ->
          putStrLn $ show term ++ "\t" ++ show (map toDocName postings)
      else
        forM_ terms $ \term ->
          case DiskIndex.lookupPostings (Types.fromString term) idx of
            Just postings ->
                putStrLn $ show term ++ "\t" ++ show (map toDocName postings)
    return ()
