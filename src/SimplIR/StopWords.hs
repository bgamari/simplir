{-# LANGUAGE TemplateHaskell #-}

module SimplIR.StopWords where

import qualified Data.Text as T
import SimplIR.ReadStopWords
import qualified Data.HashSet as HS

killStopwords :: StopWords -> [T.Text] -> [T.Text]
killStopwords stopWords =
    filter (not . (`HS.member` stopWords))

type StopWords = HS.HashSet T.Text

enInquery :: StopWords
enInquery = $$(readStopWords "inquery-en.txt")
