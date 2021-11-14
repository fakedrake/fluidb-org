{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TypeOperators     #-}
module Control.Antisthenis.Lens (HasLens(..),rgetL) where

import qualified Control.Category as C
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

instance HasLens (Min a) (Maybe a) where
  defLens = Lens { getL = \(Min a) ->
    a,modL = \f (Min a) -> Min $ f a }

instance HasLens (Sum a) (Maybe a) where
  defLens = Lens { getL = \(Sum a) ->
    a,modL = \f (Sum a) -> Sum $ f a }

rgetL :: (Zero a,HasLens a b) => b -> a
rgetL b = modL defLens (const b) zero
instance HasLens (Min a) (Min a)
instance HasLens (Sum a) (Sum a)
