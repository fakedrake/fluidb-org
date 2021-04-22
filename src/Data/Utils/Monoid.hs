{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DerivingVia #-}
module Data.Utils.Monoid (Min(..),Mul(..),Sum(..)) where

import Data.Utils.AShow
import GHC.Generics
import Data.Utils.Default

newtype Min a = Min (Maybe a)
  deriving stock (Show,Eq,Functor,Generic)
  deriving Applicative via Maybe
  deriving Default via Maybe a

instance Num a => Num (Min a) where
  signum (Min Nothing) = Min $ Just 1
  signum (Min (Just a)) = Min $ Just $ signum a
  abs (Min a) = Min $ abs <$> a
  fromInteger x = Min $ Just $ fromInteger x
  Min a + Min b = Min $ (+) <$> a <*> b
  Min a - Min b = Min $ (-) <$> a <*> b
  Min a * Min b = Min $ (*) <$> a <*> b

instance Ord a => Ord (Min a) where
  compare (Min Nothing) (Min Nothing) = EQ
  compare (Min Nothing) (Min (Just _)) = GT
  compare (Min (Just _)) (Min Nothing) = LT
  compare (Min (Just a)) (Min (Just b)) = compare a b

instance Ord a => Semigroup (Min a) where
  a <> b = min a b
instance Ord a => Monoid (Min a) where
  mempty = Min Nothing

instance AShow v => AShow (Min v)

data Mul a = Mul Int a
  deriving (Show,Eq,Generic)
instance (Num a,Ord a) => Ord (Mul a) where
  compare (Mul 0 a) (Mul 0 b) = compare a b
  compare (Mul 0 a) _ = compare a 0
  compare _ (Mul 0 b) = compare 0 b
  compare _ _ = EQ
instance Real a => Real (Mul a) where
  toRational (Mul 0 a) = toRational a
  toRational _ = 0
instance Enum a => Enum (Mul a) where
  toEnum = Mul 0 . toEnum
  fromEnum (Mul 0 x) = fromEnum x
  fromEnum _ = 0
instance Integral a => Integral (Mul a) where
  quotRem (Mul za a) (Mul zb b) =
    (Mul (za - zb) q,Mul (za - zb) r)
    where
      (q,r) = quotRem a b
  toInteger (Mul 0 a) = toInteger a
  toInteger _ = 0

instance Num a => Num (Mul a) where
  signum (Mul _ a) = Mul 0 $  signum a
  abs (Mul i a) = Mul i $ abs a
  fromInteger x = if x == 0 then Mul 1 1 else Mul 0 $ fromInteger x
  a0 + b0 = case (a0,b0) of
    (Mul 0 a,Mul 0 b) -> Mul 0 $ a + b
    (Mul za a,Mul zb b) -> case compare za zb of
      EQ -> Mul za $ a + b
      LT -> Mul za a
      GT -> Mul zb b
  a0 - b0 = case (a0,b0) of
    (Mul 0 a,Mul 0 b) -> Mul 0 $ a - b
    (Mul za a,Mul zb b) -> case compare za zb of
      EQ -> Mul za $ a - b
      LT -> Mul za a
      GT -> Mul zb b
  Mul za a * Mul zb b = Mul (za + zb) (a*b)

instance Num a => Semigroup (Mul a) where
  a <> b = a * b
instance Num a => Monoid (Mul a) where
  mempty = Mul 0 1

instance AShow v => AShow (Mul v)

newtype Sum a = Sum (Maybe a)
  deriving stock (Show,Eq,Functor,Generic)
  deriving Applicative via Maybe
  deriving Default via Maybe a

instance Num a => Num (Sum a) where
  signum (Sum Nothing) = Sum $ Just 1
  signum (Sum (Just a)) = Sum $ Just $ signum a
  abs (Sum a) = Sum $ abs <$> a
  fromInteger x = Sum $ Just $ fromInteger x
  Sum a + Sum b = Sum $ (+) <$> a <*> b
  Sum a - Sum b = Sum $ (-) <$> a <*> b
  Sum a * Sum b = Sum $ (*) <$> a <*> b

instance Ord a => Ord (Sum a) where
  compare (Sum Nothing) (Sum Nothing) = EQ
  compare (Sum Nothing) (Sum (Just _)) = GT
  compare (Sum (Just _)) (Sum Nothing) = LT
  compare (Sum (Just a)) (Sum (Just b)) = compare a b

instance Num a => Semigroup (Sum a) where
  a <> b = a + b
instance Num a => Monoid (Sum a) where
  mempty = Sum Nothing

instance AShow v => AShow (Sum v)
