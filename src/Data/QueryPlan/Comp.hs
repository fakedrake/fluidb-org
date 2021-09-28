{-# LANGUAGE TypeFamilies #-}
module Data.QueryPlan.Comp (Comp(..),nonComp,toComp) where

import           Control.Applicative
import           Data.Pointed
import           Data.QueryPlan.Scalable
import           Data.Utils.AShow
import           Data.Utils.AShow.Print
import           Data.Utils.Nat

-- | An integer binomial that assumes that we will be adding.
data Comp a = Comp { cProbNonComp :: Double,cValue :: a }
  deriving (Functor,Show,Eq)

instance AShow a => AShow (Comp a) where
  ashow' (Comp i a) =
    Sym $ show i ++ "w+" ++ showSExpOneLine False (ashow' a)

-- |Note: ord is ambiguous.
instance (Ord a,Scalable a) => Ord (Comp a) where
  compare (Comp c1 a) (Comp c2 b) =
    if
      | c1 > thr && c2 > thr -> compare (scale c1 a) (scale c2 b)
      | c1 > thr             -> GT
      | c2 > thr             -> LT
      | otherwise            -> compare a b
    where
      thr = 0.7

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

nonComp :: Zero a => Comp a
nonComp = Comp 1 zero

toComp :: a -> Comp a
toComp = Comp 0
instance Zero a => Zero (Comp a) where
  zero = point zero
  isNegative (Comp _ a) = isNegative a
