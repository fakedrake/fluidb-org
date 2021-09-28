module Data.QueryPlan.Scalable (Scalable(..)) where

import           Data.Utils.Nat

class Scalable a where
  scale :: Double -> a -> a

instance Scalable Integer where scale sc i = round $ sc * fromIntegral i
instance Scalable Int where scale sc i = round $ sc * fromIntegral i
instance Scalable Double where scale sc i = sc * i
instance Scalable a => Scalable (Min a) where scale sc (Min a) = Min $ fmap (scale sc) a
instance Scalable a => Scalable (Sum a) where scale sc (Sum a) = Sum $ fmap (scale sc) a
