{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Foldl.Vector (toVector) where

import qualified Control.Foldl as Fold
import Control.Monad.Primitive
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM

-- | Place elements into a vector.
--
-- For efficient filling, the vector is grown exponentially.
--
toVector :: forall v e m. (VG.Vector v e, PrimMonad m)
         => Fold.FoldM m e (v e)
toVector = Fold.FoldM step initial extract
  where
    initial :: m (ToVecS m v e)
    initial = ToVecS <$> VGM.new 16 <*> pure 0

    extract :: ToVecS m v e -> m (v e)
    extract (ToVecS{..}) = VG.take idx <$> VG.unsafeFreeze result

    step :: ToVecS m v e -> e -> m (ToVecS m v e)
    step (ToVecS{..}) x
      | idx == VGM.length result = do
            result' <- VGM.unsafeGrow result (VGM.length result)
            step (ToVecS result' idx) x
      | otherwise = do
            VGM.unsafeWrite result idx x
            return $ ToVecS result (idx+1)

data ToVecS m v e = ToVecS { result :: !(VG.Mutable v (PrimState m) e)
                           , idx    :: !Int
                           }
