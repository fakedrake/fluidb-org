{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TypeOperators     #-}
module Control.Antisthenis.Lens ((:>:)(..),HasLens(..),ifLt,min2l,rgetL,diff2l) where

import qualified Control.Category  as C
import           Data.Utils.Monoid
import           Data.Utils.Nat

-- | A very simple lens to handle the infinite type problem.
data a :>:  b =
  Lens
  { getL :: a -> b,modL :: (b -> b) -> a -> a }

instance C.Category (:>:) where
  a . b = Lens { getL = getL a . getL b,modL = modL b . modL a }
  id = Lens { getL = id,modL = id }

class HasLens a b where
  defLens :: a :>: b
  default defLens :: a ~ b => a :>: b
  defLens = C.id

ifLt :: (Ord b,HasLens a b) => a -> b ->  c -> c -> c
ifLt a b t e = if getL defLens a < b then t else e
min2l :: (Ord b,HasLens a b) => a -> b -> a
min2l a b = modL defLens (min b) a
diff2l :: forall a b . (Invertible b,HasLens a b) => a -> b -> a
diff2l a b = modL (defLens :: a :>: b) go a
  where
    go b' = b' `imappend` inv b
rgetL :: (Zero a,HasLens a b) => b -> a
rgetL b = modL defLens (const b) zero
