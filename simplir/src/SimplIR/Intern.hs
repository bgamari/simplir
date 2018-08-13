{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module SimplIR.Intern
    ( InternM
    , runInternM'
    , runInternM
    , internAll
    , intern
    ) where

import Data.Tuple
import Data.Hashable
import Control.Lens
import Control.Monad.Trans.State.Strict
import qualified Data.HashMap.Strict as HM

-- | The interning table
newtype Table b = Table (HM.HashMap b b)

emptyTable :: Table b
emptyTable = Table HM.empty

insert :: (Eq b, Hashable b)
       => b -> Table b -> (Table b, b)
insert b (Table m)
  | Just b' <- HM.lookup b m = (Table m, b')
  | otherwise                = (Table $ HM.insert b b m, b)

newtype InternM b a = InternM (State (Table b) a)
                    deriving (Functor, Applicative)

runInternM' :: InternM b a -> (Table b, a)
runInternM' (InternM m) = swap $ runState m emptyTable

runInternM :: InternM b a -> a
runInternM = snd . runInternM'

internAll :: (Eq b, Hashable b) => Traversal' a b -> a -> InternM b a
internAll t = traverseOf t intern

intern :: (Eq a, Hashable a) => a -> InternM a a
intern x = InternM $ state $ swap . insert x
