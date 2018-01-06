module SimplIR.DiskIndex.Posting2.TermIndex where

import qualified Data.Map.Strict as M
import Control.DeepSeq

import SimplIR.Utils.Compact
import Codec.Serialise (Serialise)
import qualified SimplIR.DiskIndex.Posting2.Internal as PIdx
import qualified SimplIR.DiskIndex.Posting2.CborList as CL
import SimplIR.Types

newtype TermIndexPath term p = TermIndexPath FilePath

data TermIndex term p = TermIndex { postingIndex :: !(PIdx.PostingIndex term p)
                                  , termIndex :: !(M.Map term (CL.Offset (PIdx.TermPostings term p)))
                                  }

build :: (Ord term, NFData term, Serialise p, Serialise term)
      => PIdx.PostingIndex term p
      -> TermIndex term p
build pidx = TermIndex pidx tidx
  where
    tidx = inCompact $ M.fromList $ map f $ CL.toListWithOffsets $ PIdx.postingIndex pidx
    f (off, PIdx.TermPostings t _) = (t, off)

lookup :: (Ord term, Serialise term, Serialise p)
       => TermIndex term p -> term -> Maybe [Posting p]
lookup tidx t = snd . PIdx.decodeTermPostings <$> lookup' tidx t

lookup' :: (Ord term, Serialise term, Serialise p)
        => TermIndex term p -> term -> Maybe (PIdx.TermPostings term p)
lookup' (TermIndex pidx tidx) t = do
    off <- M.lookup t tidx
    pure $ CL.index (PIdx.postingIndex pidx) off
