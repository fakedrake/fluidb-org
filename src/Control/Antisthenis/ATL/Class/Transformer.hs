{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}
module Control.Antisthenis.ATL.Class.Transformer
  ( ArrowTransformer(..)
  , arrLift
  , arrUnLift
  , arrHoist
  ) where

import           Control.Arrow
import           Data.Copointed
import           Data.Pointed
import           Data.Profunctor

class Copointed (TDom t) => ArrowTransformer t where
  type TCod t :: * -> *
  type TDom t :: * -> *
  lift' :: (Profunctor p,Arrow p) => p (TDom t a) (TCod t b) -> t p a b
  unlift' :: (Profunctor p,Arrow p) => t p a b -> p (TDom t a) (TCod t b)
arrLift :: (Applicative (TCod t),ArrowTransformer t,Arrow c,Profunctor c) =>
          c a b -> t c a b
arrLift a = lift' $ dimap copoint pure a
{-# INLINE arrLift #-}
arrUnLift
  :: (Pointed (TDom t)
     ,Copointed (TCod t)
     ,ArrowTransformer t
     ,Arrow c
     ,Profunctor c)
  => t c a b
  -> c a b
arrUnLift a = dimap point copoint $ unlift' a
{-# INLINE arrUnLift #-}
arrHoist
  :: (Arrow c,Arrow c',ArrowTransformer t,Profunctor c,Profunctor c')
  => (c (TDom t a) (TCod t b) -> c' (TDom t a) (TCod t b))
  -> t c a b
  -> t c' a b
arrHoist f = lift' . f . unlift'
