{-# LANGUAGE TemplateHaskell #-}

module SimplIR.StopWords where

import qualified Data.Text as T
import SimplIR.StopWords.Read
import qualified Data.HashSet as HS

killStopwords :: StopWords -> [T.Text] -> [T.Text]
killStopwords stopWords =
    filter (not . (`HS.member` stopWords))

killStopwords' :: StopWords -> (a -> T.Text) -> [a] -> [a]
killStopwords' stopWords f =
    filter (not . (`HS.member` stopWords) . f)

type StopWords = HS.HashSet T.Text

enInquery :: StopWords
enInquery = $$(readStopWords "inquery-en.txt")
