module Data.QueryPlan.HistBnd
  (HistCap(..)
  ,HistVal(..)
  ,HistCapI(..)
  ,nonComp
  ,maxCap
  ,scaleSum
  ,unscaleCap
  ,scaleHistVal,maxMatTrail,exceedsHistCap,getHistCapI) where

import           Control.Antisthenis.Types
import qualified Data.IntSet               as IS
import           Data.Pointed
import           Data.QueryPlan.CostTypes
import           Data.Utils.AShow
import           Data.Utils.Embed
import           Data.Utils.Nat
import           GHC.Generics

data HistVal a =
  HistVal { hvMaxMatTrail :: IS.IntSet,hvVal :: a,hvNonComp :: Double }
  deriving (Functor,Generic,Eq)
instance AShow a => AShow (HistVal a)
instance Zero a => Zero (HistVal a) where
  zero = point zero
  isNegative hv = isNegative $ hvVal hv
nonComp :: Zero a => HistVal a
nonComp = zero { hvNonComp = 1 }

data HistCapI a =
  HistCapI
  { hcMatsEncountered :: Int,hcValCap :: Min a,hcNonCompTolerance :: Double }
  deriving Generic

data HistCap a
  = HCOr (HistCapI a) (HistVal a)
  | HCCap (HistCapI a)
  | HCVal (HistVal a)
  deriving Generic

instance AShow a => AShow (HistCapI a)
instance AShow a => AShow (HistCap a)
instance Zero a => Zero (HistCapI a) where
  zero =
    HistCapI { hcMatsEncountered = 0,hcValCap = zero,hcNonCompTolerance = 0.7 }
  isNegative hc = isNegative $ hcValCap hc
instance Zero a => Zero (HistCap a) where
  zero = HCCap zero
  isNegative hc = case hc of
    HCOr a b -> isNegative a || isNegative b
    HCCap a  -> isNegative a
    HCVal a  -> isNegative a

instance Semigroup a => Semigroup (HistVal a) where
  hv <> hv' =
    HistVal { hvMaxMatTrail = hvMaxMatTrail hv <> hvMaxMatTrail hv'
             ,hvVal = hvVal hv <> hvVal hv'
             ,hvNonComp = 1 - (hvNonComp hv - 1) * (hvNonComp hv' - 1)
            }

instance Subtr2 a b => Subtr2 (HistVal a) (HistVal b)  where
  subtr hv hv' =
    HistVal
    { hvMaxMatTrail = IS.difference (hvMaxMatTrail hv) (hvMaxMatTrail hv')
     ,hvVal = subtr (hvVal hv) (hvVal hv')
     ,hvNonComp = 1 + (hvNonComp hv - 1) / (hvNonComp hv' - 1)
    }

instance Ord a => Ord (HistVal a) where
  compare hv hv' = compare (mkTup hv) (mkTup hv')
    where
      mkTup HistVal {..} = (hvNonComp,hvVal)

instance Pointed HistVal where
  point a = HistVal { hvMaxMatTrail = mempty,hvVal = a,hvNonComp = 0 }

-- | The cap counts how many materialized nodes WE HAVE ENCOUNTERED.
maxCap :: a -> HistCap a
maxCap maxCost =
  HCCap
    HistCapI
    { hcMatsEncountered = 1
     ,hcValCap = Min $ Just maxCost
     ,hcNonCompTolerance = 0.7
    }


-- | Double the cap because we know we will be scaling afterwards.
unscaleCap :: HistCap Cost -> HistCap Cost
unscaleCap (HCOr hci hv) = HCOr (unscaleCapI hci) (unscaleBnd hv)
unscaleCap (HCCap hci)   = HCCap $ unscaleCapI hci
unscaleCap (HCVal hv)    = HCVal $ unscaleBnd hv

unscaleBnd :: HistVal Cost -> HistVal Cost
unscaleBnd hv =
  hv { hvVal = unscaleCost $ hvVal hv
      ,hvMaxMatTrail = IS.mapMonotonic (+1) $ hvMaxMatTrail hv
     }

unscaleCapI :: HistCapI Cost -> HistCapI Cost
unscaleCapI hc =
  hc { hcValCap = Min $ fmap unscaleCost $ getMin $ hcValCap hc
      ,hcMatsEncountered = 1 + hcMatsEncountered hc
     }

unscaleCost :: Cost -> Cost
unscaleCost (Cost r w) = Cost (double r) (double w) where
  double :: Int -> Int
  double i = if i < 0 then i else i * 2

-- | Half the value because it is actually materialized.
scaleHistVal :: HistVal Cost -> HistVal Cost
scaleHistVal hv = hv{hvVal=scaleCost $ hvVal hv}
scaleSum :: Sum (HistVal Cost) -> Sum (HistVal Cost)
scaleSum m@(Sum Nothing) = m
scaleSum (Sum (Just hv)) = point $ scaleHistVal hv
halfCeil :: Int -> Int
halfCeil i =
  if
    | i <= 0    -> i
    | m > 0     -> d + 1
    | otherwise -> d
  where
    (d,m) = divMod i 2
scaleCost :: Cost -> Cost
scaleCost (Cost r w) = Cost (halfCeil r) (halfCeil w)

maxMatTrail :: Int
maxMatTrail = 4

exceedsHistCap :: Ord a => HistCap a -> HistVal a -> ExceedsCap (HistCap a)
exceedsHistCap (HCOr hci hv) v = case (compare hv v,exceedsHistCapI hci v) of
  (LT,_)               -> BndExceeds
  (_,BndExceeds)       -> BndExceeds
  (EQ,_)               -> EqualCap
  (GT,BndWithinCap v') -> BndWithinCap $ HCOr hci v'
  (GT,EqualCap)        -> EqualCap
exceedsHistCap (HCCap hci) v   = HCVal <$> exceedsHistCapI hci v
exceedsHistCap (HCVal hv) v    = case compare hv v of
  EQ -> EqualCap
  LT -> BndExceeds
  GT -> BndWithinCap $ HCVal v

exceedsHistCapI :: Ord a => HistCapI a -> HistVal a -> ExceedsCap (HistVal a)
exceedsHistCapI HistCapI {..} bnd = case compBndCap of
  (GT,_,_)   -> BndExceeds
  (_,GT,_)   -> BndExceeds
  (_,_,GT)   -> BndExceeds
  (EQ,EQ,EQ) -> EqualCap
  (_,_,_)    -> BndWithinCap bnd
  where
    compBndCap =
      (compare bndVal hcValCap   -- reverse
      ,compare (maxMatTrail - hcMatsEncountered) bndTrailSize
      ,compare bndNonComp hcNonCompTolerance)
    bndNonComp = hvNonComp bnd
    bndVal = point $ hvVal bnd
    bndTrailSize = maybe 0 fst $ IS.maxView $ hvMaxMatTrail bnd

getHistCapI :: HistCap a -> Maybe (HistCapI a)
getHistCapI = \case
  HCCap c  -> Just c
  HCVal _  -> Nothing
  HCOr c _ -> Just c

instance Embed (Min (HistVal a)) (HistCap a) where
  emb (Min Nothing) =
    HCCap
      HistCapI
      { hcMatsEncountered = 0,hcValCap = MinInf,hcNonCompTolerance = 1 }
  emb (Min (Just hv)) = HCVal hv

-- | Used by sum to deduce the partial value from the final.
instance Subtr2 a b => Subtr2 (HistCapI a) (HistVal b) where
  subtr HistCapI {..} HistVal {..} =
    HistCapI
    { hcMatsEncountered = hcMatsEncountered
     ,hcValCap = case hcValCap of
         Min (Just v) -> point $ subtr v hvVal
         Min Nothing  -> MinInf
     ,hcNonCompTolerance = hcNonCompTolerance
    }
instance Subtr2 a b => Subtr2 (HistCap a) (HistVal b) where
  subtr (HCOr hci hv') hv = HCOr (subtr hci hv) (subtr hv' hv)
  subtr (HCCap hci) hv    = HCCap $ subtr hci hv
  subtr (HCVal hv') hv    = HCVal $ subtr hv' hv
