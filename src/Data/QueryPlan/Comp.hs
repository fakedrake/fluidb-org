{-# LANGUAGE TypeFamilies #-}
module Data.QueryPlan.Comp (Comp(..),nonComp,toComp) where

import           Control.Applicative
import           Data.Pointed
import           Data.Utils.AShow
import           Data.Utils.AShow.Print
import           Data.Utils.Monoid
import           Data.Utils.Nat

-- | An integer binomial that assumes that we will be adding.
data Comp a = Comp Double a deriving (Functor,Show,Eq)

instance AShow a => AShow (Comp a) where
  ashow' (Comp i a) =
    Sym $ show i ++ "w+" ++ showSExpOneLine False (ashow' a)
instance Ord a => Ord (Comp a) where
  compare (Comp _ a) (Comp _ b) = compare a b

instance Applicative Comp where
  pure = Comp 0
  Comp c f <*> Comp c' v = Comp (1 - (c - 1) * (c' - 1)) $ f v

instance Num a => Num (Comp a) where
  (+) = liftA2 (+)
  (-) = liftA2 (-)
  (*) = liftA2 (*)
  negate = fmap negate
  abs = fmap abs
  signum = fmap signum
  fromInteger = pure . fromInteger

instance Pointed Comp where
  point = pure
instance Subtr a => Subtr (Comp a) where
  subtr = liftA2 subtr

instance Semigroup a => Semigroup (Comp a) where
  Comp i a <> Comp i' a' =
    Comp (1 - (1 - i) * (1 - i')) (a <> a')
instance Monoid a => Monoid (Comp a) where
  mempty = Comp 0 mempty

nonComp :: Monoid a => Comp a
nonComp = Comp 1 mempty

toComp :: a -> Comp a
toComp = Comp 0
instance Zero a => Zero (Comp a) where zero = point zero
