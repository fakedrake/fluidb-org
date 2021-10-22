{-# LANGUAGE DefaultSignatures    #-}
{-# LANGUAGE DeriveFunctor        #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE DerivingVia          #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
module Data.Utils.Monoid
  (Min'(..)
  ,Min(..)
  ,Mul(..)
  ,Sum(..)
  ,Max(..)
  ,Invertible(..)) where

import           Data.Utils.AShow
import           Data.Utils.Default
import           GHC.Generics

-- | Minimum of natural numbers.
newtype Min' a = Min' a
  deriving stock (Show,Eq,Functor,Generic)
instance Num a => Num (Min' a) where
  signum (Min' a) = Min' $ signum a
  abs (Min' a) = Min' $ abs a
  fromInteger x = Min' $ fromInteger x
  Min' a + Min' b = Min' $ a + b
  Min' a - Min' b = Min' $ a - b
  Min' a * Min' b = Min' $ a * b

instance Ord a => Ord (Min' a) where
  compare (Min' a) (Min' b) = compare a b
  {-# INLINE compare #-}

instance Ord a => Semigroup (Min' a) where
  Min' a <> Min' b = Min' $ min a b
  {-# INLINE (<>) #-}

instance AShow v => AShow (Min' v)

newtype Min a = Min (Maybe a)
  deriving stock (Show,Eq,Functor,Generic)
  deriving Applicative via Maybe
  deriving Default via Maybe a

instance Num a => Num (Min a) where
  signum (Min Nothing)  = Min $ Just 1
  signum (Min (Just a)) = Min $ Just $ signum a
  abs (Min a) = Min $ abs <$> a
  fromInteger x = Min $ Just $ fromInteger x
  Min a + Min b = Min $ (+) <$> a <*> b
  Min a - Min b = Min $ (-) <$> a <*> b
  Min a * Min b = Min $ (*) <$> a <*> b

instance Ord a => Ord (Min a) where
  compare (Min Nothing) (Min Nothing)   = EQ
  compare (Min Nothing) (Min (Just _))  = GT
  compare (Min (Just _)) (Min Nothing)  = LT
  compare (Min (Just a)) (Min (Just b)) = compare a b
  {-# INLINE compare #-}

instance Ord a => Semigroup (Min a) where
  a <> b = min a b
  {-# INLINE (<>) #-}
instance Ord a => Monoid (Min a) where
  mempty = Min Nothing
  {-# INLINE mempty #-}

instance AShow v => AShow (Min v)

newtype Max a = Max (Maybe a)
  deriving stock (Show,Eq,Functor,Generic)
  deriving Applicative via Maybe
  deriving Default via Maybe a

instance Num a => Num (Max a) where
  signum (Max Nothing)  = Max $ Just 1
  signum (Max (Just a)) = Max $ Just $ signum a
  abs (Max a) = Max $ abs <$> a
  fromInteger x = Max $ Just $ fromInteger x
  Max a + Max b = Max $ (+) <$> a <*> b
  Max a - Max b = Max $ (-) <$> a <*> b
  Max a * Max b = Max $ (*) <$> a <*> b

instance Ord a => Ord (Max a) where
  compare (Max Nothing) (Max Nothing)   = EQ
  compare (Max Nothing) (Max (Just _))  = LT
  compare (Max (Just _)) (Max Nothing)  = GT
  compare (Max (Just a)) (Max (Just b)) = compare a b

instance Ord a => Semigroup (Max a) where
  a <> b = max a b
instance Ord a => Monoid (Max a) where
  mempty = Max Nothing

instance AShow v => AShow (Max v)


data Mul a = Mul Int a
  deriving (Show,Eq,Generic)
instance (Num a,Ord a) => Ord (Mul a) where
  compare (Mul 0 a) (Mul 0 b) = compare a b
  compare (Mul 0 a) _         = compare a 0
  compare _ (Mul 0 b)         = compare 0 b
  compare _ _                 = EQ
instance Real a => Real (Mul a) where
  toRational (Mul 0 a) = toRational a
  toRational _         = 0
instance Enum a => Enum (Mul a) where
  toEnum = Mul 0 . toEnum
  fromEnum (Mul 0 x) = fromEnum x
  fromEnum _         = 0
instance Integral a => Integral (Mul a) where
  quotRem (Mul za a) (Mul zb b) =
    (Mul (za - zb) q,Mul (za - zb) r)
    where
      (q,r) = quotRem a b
  toInteger (Mul 0 a) = toInteger a
  toInteger _         = 0

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

-- Nothing means infinity
newtype Sum a = Sum (Maybe a)
  deriving stock (Show,Eq,Functor,Generic)
  deriving Applicative via Maybe
  deriving Default via Maybe a

instance Num a => Num (Sum a) where
  signum (Sum Nothing)  = Sum $ Just 1
  signum (Sum (Just a)) = Sum $ Just $ signum a
  abs (Sum a) = Sum $ abs <$> a
  fromInteger x = Sum $ Just $ fromInteger x
  Sum a + Sum b = Sum $ (+) <$> a <*> b
  Sum a - Sum b = Sum $ (-) <$> a <*> b
  Sum a * Sum b = Sum $ (*) <$> a <*> b

instance Ord a => Ord (Sum a) where
  compare (Sum Nothing) (Sum Nothing)   = EQ
  compare (Sum Nothing) (Sum (Just _))  = GT
  compare (Sum (Just _)) (Sum Nothing)  = LT
  compare (Sum (Just a)) (Sum (Just b)) = compare a b

-- | XXX: This is actually wrong if Nothing is infinity.
instance Num a => Semigroup (Sum a) where
  a <> b = a + b
instance Num a => Monoid (Sum a) where
  mempty = Sum $ Just 0

instance AShow v => AShow (Sum v)

-- | Invertible number.
class (Monoid a) => Invertible a where
  type Inverse a :: *
  inv :: a -> Inverse a
  uninv :: Inverse a -> a
  imappend :: a -> Inverse a -> a
