{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}

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
import Control.Monad.Trans.Class
import Control.Monad.IO.Class
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

newtype InternM b m a = InternM (StateT (Table b) m a)
                      deriving (Functor, Applicative, Monad, MonadIO, MonadTrans)

runInternM' :: forall b m a. Functor m => InternM b m a -> m (Table b, a)
runInternM' (InternM m) = swap <$> runStateT m emptyTable

runInternM :: forall b m a. Functor m => InternM b m a -> m a
runInternM = fmap snd . runInternM'

internAll :: (Monad m, Eq b, Hashable b)
          => Optical (->) (->) (InternM b m) a a b b  -- ^ e.g. @Traversal a a b b@
          -> a -> InternM b m a
internAll t = traverseOf t intern

intern :: (Monad m, Eq a, Hashable a) => a -> InternM a m a
intern x = InternM $ state $ swap . insert x
