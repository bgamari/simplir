{-# LANGUAGE CPP #-}

#if __GLASGOW_HASKELL__ > 802
#define USE_COMPACT
#endif

module SimplIR.Utils.Compact
    ( inCompactM
    , inCompact
    ) where

import Control.DeepSeq
#ifdef USE_COMPACT
import GHC.Compact
#endif

inCompactM :: NFData a => IO a -> IO a
inCompact :: NFData a => a -> a
#ifdef USE_COMPACT
inCompactM action = action >>= fmap getCompact . compact
inCompact         = unsafePerformIO . fmap getCompact . compact
#else
inCompactM = id
inCompact  = id
#endif
