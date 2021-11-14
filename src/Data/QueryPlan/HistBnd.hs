module Data.QueryPlan.HistBnd (HistCap(..),HistVal(..),nonComp) where

import           Control.Antisthenis.Lens
import qualified Data.IntSet              as IS
import           Data.Pointed
import           Data.Utils.AShow
import           Data.Utils.Nat
import           GHC.Generics

data HistCap a =
  HistCap
  { hcMatsEncountered :: Int,hcValCap :: Min a,hcNonCompTolerance :: Double }
  deriving Generic
instance AShow a => AShow (HistCap a)
instance Zero a => Zero (HistCap a) where
  zero =
    HistCap { hcMatsEncountered = 0,hcValCap = zero,hcNonCompTolerance = 0.7 }
  isNegative = isNegative . hcValCap
instance HasLens (HistCap a) (Min (HistVal a)) where
  defLens =
    Lens { getL = undefined,modL = \f c -> c { hcValCap = undefined } }


data HistVal a =
  HistVal { hvMaxMatTrail :: IS.IntSet,hvVal :: a,hvNonComp :: Double }
  deriving (Functor,Generic,Eq)
instance AShow a => AShow (HistVal a)
instance Zero a => Zero (HistVal a) where
  zero = point zero
  isNegative = isNegative . hvVal
instance HasLens (HistVal a) a where
  defLens = Lens { getL = hvVal,modL = \f c -> c { hvVal = f $ hvVal c } }
nonComp :: Zero a => HistVal a
nonComp = zero { hvNonComp = 1 }


instance Semigroup a => Semigroup (HistVal a) where
  hv <> hv' =
    HistVal { hvMaxMatTrail = hvMaxMatTrail hv <> hvMaxMatTrail hv'
             ,hvVal = hvVal hv <> hvVal hv'
             ,hvNonComp = 1 - (hvNonComp hv - 1) * (hvNonComp hv' - 1)
            }

instance Subtr a => Subtr (HistVal a) where
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
