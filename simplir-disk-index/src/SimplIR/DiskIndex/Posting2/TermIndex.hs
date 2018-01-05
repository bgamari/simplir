module SimplIR.DiskIndex.Posting2.TermIndex where

import qualified Data.HashMap.Strict as HM
import Data.Hashable (Hashable)

import Codec.Serialise (Serialise)
import qualified SimplIR.DiskIndex.Posting2 as PIdx
import qualified SimplIR.DiskIndex.Posting2.CborList as CL
import SimplIR.Types

newtype TermIndexPath term p = TermIndexPath FilePath

data TermIndex term p = TermIndex { postingIndex :: !(PIdx.PostingIndex term p)
                                  , termIndex :: !(HM.HashMap term (CL.Offset (PIdx.TermPostings term p)))
                                  }

build :: (Hashable term, Eq term, Serialise p, Serialise term)
      => PIdx.PostingIndex term p
      -> TermIndex term p
build pidx = TermIndex pidx tidx
  where
    tidx = HM.fromList $ map f $ CL.toListWithOffsets $ PIdx.postingIndex pidx
    f (off, PIdx.TermPostings t _) = (t, off)

lookup :: (Hashable term, Eq term, Serialise term, Serialise p)
       => TermIndex term p -> term -> Maybe [Posting p]
lookup tidx t = snd . PIdx.decodeTermPostings <$> lookup' tidx t

lookup' :: (Hashable term, Eq term, Serialise term, Serialise p)
        => TermIndex term p -> term -> Maybe (PIdx.TermPostings term p)
lookup' (TermIndex pidx tidx) t = do
    off <- HM.lookup t tidx
    pure $ CL.index (PIdx.postingIndex pidx) off
