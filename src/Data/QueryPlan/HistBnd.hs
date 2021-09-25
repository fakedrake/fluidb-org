module Data.QueryPlan.HistBnd
  (HistCap(..)) where

import           Control.Antisthenis.Lens
import           Data.Pointed
import           Data.QueryPlan.Cert
import           Data.QueryPlan.Comp
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
instance HasLens (HistCap a) (Min a) where
  defLens =
    Lens { getL = hcValCap,modL = \f c -> c { hcValCap = f $ hcValCap c } }

instance HasLens (HistCap a) (Min (Cert (Comp a))) where
  defLens =
    Lens
    { getL = Min . fmap (point . point) . unMin . hcValCap,modL = modHistCap }

modHistCap
  :: (Min (Cert (Comp a)) -> Min (Cert (Comp a))) -> HistCap a -> HistCap a
modHistCap f hc =
  hc
  { hcValCap = Min
      $ fmap (cValue . cData)
      $ unMin
      $ f
      $ Min
      $ fmap (point . point)
      $ unMin
      $ hcValCap hc
  }

data HistBnd a = HistBnd { hbMaxMatTrail :: Int,hbVal :: Min a }
  deriving Generic
instance AShow a => AShow (HistBnd a)
instance Zero a => Zero (HistBnd a) where
  zero = HistBnd { hbMaxMatTrail = 0,hbVal = zero }
  isNegative = isNegative . hbVal
instance HasLens (HistBnd a) (Min a) where
  defLens = Lens { getL = hbVal,modL = \f c -> c { hbVal = f $ hbVal c } }
