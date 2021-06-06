{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
module Control.Antisthenis.ATL.Class.Functorial
  (ArrowFunctor(..)) where

import Control.Arrow

-- | A functor corresponding to an arrow.x
class Functor (ArrFunctor c) => ArrowFunctor c where
  type ArrFunctor c :: * -> *

  toKleisli :: c a b -> a -> ArrFunctor c b
  fromKleisli :: (a -> ArrFunctor c b) -> c a b


instance Functor m => ArrowFunctor (Kleisli m) where
  type ArrFunctor (Kleisli m) = m
  toKleisli (Kleisli c) = c
  fromKleisli = Kleisli
