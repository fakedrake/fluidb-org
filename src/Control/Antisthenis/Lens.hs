{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE RankNTypes #-}
module Control.Antisthenis.Lens (getL,modL,(:>:)(..)) where

import Data.Utils.Const
import Control.Monad.Identity
import qualified Control.Category as C

-- | A very simple lens to handle the infinite type problem.
newtype a :>:  b =
  Lens' { runLens' :: forall f . Functor f => (b -> f b) -> a -> f a }

instance C.Category (:>:) where
  Lens' f . Lens' g = Lens' $ g . f
  id = Lens' id

getL :: a :>: b -> a -> b
getL (Lens' l) = getConst . l Const
modL :: a :>: b -> (b -> b) -> a -> a
modL (Lens' l) f = runIdentity . l (Identity . f)
