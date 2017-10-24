{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

-- | Measures of inter-annotator agreement.
module SimplIR.Assessment.Agreement where

import Data.Maybe
import Data.Hashable
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS

-- | Compute Cohen's κ measure.
cohenKappa :: forall subj cat. (Eq cat, Hashable cat, Eq subj, Hashable subj)
           => HM.HashMap subj cat -- ^ assessments of assessor A
           -> HM.HashMap subj cat -- ^ assessments of assessor B
           -> Double -- ^ κ
cohenKappa a b =
    1 - (1 - po) / (1 - pe)
  where
    !agreementCount = length [ ()
                             | (ka,kb) <- HM.elems inter
                             , ka == kb
                             ]
    !po = realToFrac agreementCount / realToFrac allCount
    !pe = realToFrac (sum $ HM.intersectionWith (*) na nb) / realToFrac (squared allCount)

    -- how many times a and b predict class k
    na, nb :: HM.HashMap cat Int
    !na = HM.fromListWith (+) [ (k, 1) | (_, (k, _)) <- HM.toList inter ]
    !nb = HM.fromListWith (+) [ (k, 1) | (_, (_, k)) <- HM.toList inter ]

    inter :: HM.HashMap subj (cat,cat)
    !inter = HM.intersectionWith (,) a b
    !allCount = HM.size inter

-- | Compute Cohen's kappa measure, allowing for arbitrary agreement classes.
cohenKappa' :: forall subj cat. (Eq cat, Hashable cat, Eq subj, Hashable subj)
            => [HS.HashSet cat]
            -> HM.HashMap subj cat -- ^ assessments of assessor A
            -> HM.HashMap subj cat -- ^ assessments of assessor B
            -> Double -- ^ κ
cohenKappa' equivs a b =
    1 - (1 - po) / (1 - pe)
  where
    po = realToFrac (length [ ()
                            | (x,y) <- HM.elems inter
                            , any (\s -> x `HS.member` s && y `HS.member` s) equivs
                            ]) / realToFrac allCount

    pe = sum [ realToFrac cntA * realToFrac cntB
             | (k1,k2) <- HS.toList $ HS.fromList
                          [ (x,y)
                          | agreementClass <- equivs
                          , x <- HS.toList agreementClass
                          , y <- HS.toList agreementClass
                          ]
             , let cntA = fromMaybe 0 $ HM.lookup k1 na
             , let cntB = fromMaybe 0 $ HM.lookup k2 nb
             ] / realToFrac (squared allCount)

    -- how many times a and b predict class k
    na, nb :: HM.HashMap cat Int
    !na = HM.fromListWith (+) [ (k, 1) | (_, (k, _)) <- HM.toList inter ]
    !nb = HM.fromListWith (+) [ (k, 1) | (_, (_, k)) <- HM.toList inter ]

    inter :: HM.HashMap subj (cat,cat)
    !inter = HM.intersectionWith (,) a b
    !allCount = HM.size inter

-- | Compute Fleiss's κ measure.
fleissKappa :: forall subj cat. (Eq cat, Hashable cat, Eq subj, Hashable subj)
            => [HM.HashMap subj cat]  -- ^ the assessments of each assessor
            -> Double -- ^ κ
fleissKappa assessments' =
    (barP - barPe) / (1 - barPe)
  where
    assessments = onlyOverlappingAssessments assessments'

    -- n_i
    numAssessments :: HM.HashMap subj Int
    numAssessments = HM.fromListWith (+) [ (x, 1)
                                         | a <- assessments
                                         , x <- HM.keys a ]
    -- N
    numSubjects = HM.size numAssessments
    totalAssessments = sum numAssessments

    -- n_ij
    nij :: HM.HashMap subj (HM.HashMap cat Int)
    nij = HM.fromListWith (HM.unionWith (+))
        [ (x, HM.singleton k 1)
        | a <- assessments
        , (x, k) <- HM.toList a
        ]

    -- p_j, probability that class k is predicted
    pj :: HM.HashMap cat Double
    pj = HM.fromListWith (+)
         [ (k, realToFrac n / realToFrac totalAssessments)
         | xs <- HM.elems nij
         , (k, n) <- HM.toList xs
         ]

    barP :: Double
    barP = (/ realToFrac numSubjects) $ sum
           [ (inner / realToFrac ni / realToFrac (ni - 1)) - 1 / realToFrac (ni - 1)
           | (x, njs) <- HM.toList nij
           , let Just ni = x `HM.lookup` numAssessments
           , let inner = sum [ realToFrac $ squared v
                             | v <- HM.elems njs
                             ]
           ]

    barPe = sum [ v^(2 :: Int) | v <- HM.elems pj ]

onlyOverlappingAssessments
    :: forall subj cat. (Eq cat, Hashable cat, Eq subj, Hashable subj)
    => [HM.HashMap subj cat] -> [HM.HashMap subj cat]
onlyOverlappingAssessments assessments =
    map (HM.filterWithKey overlaps) assessments
  where
    numAssessments :: HM.HashMap subj Int
    numAssessments = HM.fromListWith (+) [ (x, 1)
                                         | a <- assessments
                                         , x <- HM.keys a ]
    overlaps x _ = n > 1
      where Just n = x `HM.lookup` numAssessments

squared :: Num a => a -> a
squared x = x*x
