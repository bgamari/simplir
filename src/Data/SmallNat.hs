{-# LANGUAGE BangPatterns #-}

module Data.SmallNat where

import Data.Bits
import Data.Binary
import Test.QuickCheck hiding ((.&.))

-- | Efficiently encode natural numbers from 0 to 2^62 - 1
newtype SmallNat = SmallNat Word
                 deriving (Eq, Ord, Show, Read)

instance Bounded SmallNat where
    minBound = SmallNat 0
    maxBound = SmallNat (bit 62 - 1)

instance Enum SmallNat where
    fromEnum (SmallNat i) = fromIntegral i
    toEnum i
      | i >= minBound && i < maxBound = SmallNat $ fromIntegral i
      | otherwise                     = error "toEnum(SmallNat): Invalid SmallNat"

-- | Encoding scheme
--
-- > Fits in 6 bits:  00zz zzzz
-- >   = z
-- > Fits in 14 bits: 01zz zzzz  yyyy yyyy
-- >   = (z << 8) | y
-- > Fits in 30 bits: 10zz zzzz  yyyy yyyy  xxxx xxxx  wwww wwww
-- >   = (z << 24) | (y << 16) | (x << 8) | w
-- > Fits in 62 bits: 11zz zzzz  yyyy yyyy  ...
-- >   = (z << 56) | (y << 48) | (x << 40) | ...
instance Binary SmallNat where
    put (SmallNat n)
      | n < bit 6  =    putW $ withTag 0 n
      | n < bit 14 = do putW $ withTag 1 (n `shiftR` 8)
                        putW n
      | n < bit 30 = do putW $ withTag 2 (n `shiftR` 24)
                        putW $ n `shiftR` 16
                        putW $ n `shiftR` 8
                        putW $ n
      | n < bit 62 = do putW $ withTag 3 (n `shiftR` 56)
                        putW $ n `shiftR` 48
                        putW $ n `shiftR` 40
                        putW $ n `shiftR` 32
                        putW $ n `shiftR` 24
                        putW $ n `shiftR` 16
                        putW $ n `shiftR` 8
                        putW $ n
      | otherwise  =    fail "put(SmallNat): Invalid SmallNat"
      where
        withTag tag n = (tag `shiftL` 6) .|. n
        putW = putWord8 . fromIntegral

    get = do
      b <- getWord8
      let tag = b `shiftR` 6
          rem_bytes = 2^tag - 1
          z   = b .&. (bit 6 - 1)
          getWords n0 = go n0 (fromIntegral z `shiftL` (8*n0))
            where
              go 0 !acc = pure acc
              go n !acc = do
                  x <- (`shiftL` (8*n-8)) . fromIntegral <$> getWord8
                  go (n-1) (acc .|. x)
      SmallNat <$> getWords rem_bytes

instance Arbitrary SmallNat where
    arbitrary = SmallNat <$> choose (0, bit 62 - 1)

testBinary :: SmallNat -> Property
testBinary n = counterexample (show (n, n')) (n' == n)
  where
    n' = decode (encode n)
