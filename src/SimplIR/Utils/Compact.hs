{-# LANGUAGE CPP #-}

module SimplIR.Utils.Compact (inCompact) where

import Control.DeepSeq
#if __GLASGOW_HASKELL__ > 802
import GHC.Compact
#endif

inCompact :: NFData a => IO a -> IO a
#if __GLASGOW_HASKELL__ > 802
inCompact action = action >>= fmap getCompact . compact
#else
inCompact = id
#endif
