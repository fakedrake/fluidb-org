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

class Copointed (TDom t) => ArrowTransformer t where
  type TCod t :: * -> *
  type TDom t :: * -> *
  lift' :: Arrow p => p (TDom t a) (TCod t b) -> t p a b
  unlift' :: Arrow p => t p a b -> p (TDom t a) (TCod t b)
arrLift :: (Applicative (TCod t),ArrowTransformer t,Arrow c) =>
          c a b -> t c a b
arrLift a = lift' $ arr copoint >>> a >>> arr pure
{-# INLINE arrLift #-}
arrUnLift :: (Pointed (TDom t),Copointed (TCod t),ArrowTransformer t,Arrow c) =>
            t c a b -> c a b
arrUnLift a = arr point >>> unlift' a >>> arr copoint
{-# INLINE arrUnLift #-}
arrHoist
  :: (Arrow c,Arrow c',ArrowTransformer t)
  => (c (TDom t a) (TCod t b) -> c' (TDom t a) (TCod t b))
  -> t c a b
  -> t c' a b
arrHoist f = lift' . f . unlift'
