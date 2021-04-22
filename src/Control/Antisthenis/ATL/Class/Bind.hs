{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TypeFamilies           #-}

module Control.Antisthenis.ATL.Class.Bind
  ( ArrowBind(..)
  , ArrowFMap(..)
  , arrBindM
  ) where

import           Control.Arrow
import           Control.Category as Cat
import           Control.Monad

arrBindM :: ArrowBind m c => c a (m b) -> c (m a) (m b)
arrBindM c = arrBind c Cat.id
{-# INLINE arrBindM #-}
class Arrow c => ArrowBind f c where
  arrBind :: c y (f z) -> c x (f y) -> c x (f z)
  arrReturn :: c x (f x)
class (Functor f,Arrow c) => ArrowFMap f c where
  arrFMap :: c x y -> c (f x) (f y)
instance Functor f => ArrowFMap f (->) where
  arrFMap = fmap

instance Monad f => ArrowBind f (->) where
  arrBind = (<=<)
  arrReturn = return
  {-# INLINE arrBind #-}
  {-# INLINE arrReturn #-}
