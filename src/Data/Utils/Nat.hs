{-# LANGUAGE PatternSynonyms   #-}
{-# OPTIONS_GHC -Wno-missing-signatures #-}
{-# LANGUAGE DefaultSignatures #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
module Data.Utils.Nat
  (Sum(..)
  ,Min(..)
  ,Neg
  ,Zero(..)
  ,pattern SumInf
  ,pattern MinInf
  ,negSum
  ,negMin
  ,sub
  ,Subtr2(..)
  ,Subtr
  ,sumMap
  ,minMap) where

-- Use newtypes so they are coercible. Nothing means infinte
import           Data.Coerce
import           Data.Pointed
import           Data.Utils.AShow
import           Data.Utils.MinElem
import           GHC.Generics

newtype Sum a = Sum { getSum :: Maybe a } deriving (Show,Eq,Generic)
newtype Min a = Min { getMin :: Maybe a }
  deriving (Show,Eq,Generic)

instance (Ord a,Zero a) => MinElem (Sum a) where
  minElem = Sum $ Just zero
instance (Ord a,Zero a) => MinElem (Min a) where
  minElem = Min $ Just zero

newtype Neg a = Neg a
  deriving (Show,Eq,Generic)
pattern SumInf :: Sum a
pattern SumInf = Sum Nothing
pattern MinInf :: Min a
pattern MinInf = Min Nothing

instance AShow a => AShow (Sum a)
instance AShow a => AShow (Min a)
instance AShow a => AShow (Neg a)

instance Ord a => Ord (Sum a) where
  compare SumInf SumInf                 = EQ
  compare SumInf _                      = GT
  compare _ SumInf                      = LT
  compare (Sum (Just a)) (Sum (Just b)) = compare a b

instance Ord a => Ord (Min a) where
  compare MinInf MinInf                 = EQ
  compare MinInf _                      = GT
  compare _ MinInf                      = LT
  compare (Min (Just a)) (Min (Just b)) = compare a b

instance Semigroup a => Semigroup (Sum a) where
  SumInf <> _                  = SumInf
  _ <> SumInf                  = SumInf
  Sum (Just a) <> Sum (Just b) = Sum $ Just $ a <> b
instance Ord a => Semigroup (Min a) where
  MinInf <> _                  = MinInf
  _ <> MinInf                  = MinInf
  Min (Just a) <> Min (Just b) = Min $ Just $ min a b

negSum :: Sum a -> Maybe (Neg a)
negSum = fmap Neg . coerce
negMin :: Min a -> Maybe (Neg a)
negMin = fmap Neg . coerce
sub :: Subtr2 a b => Sum a -> Neg b -> Sum a
sub SumInf _               = SumInf
sub (Sum (Just a)) (Neg b) = Sum $ Just $ subtr a b

class Zero a where
  zero  :: a
  default zero :: Num a => a
  zero = 0
  isNegative :: a -> Bool
  default isNegative :: Ord a => a -> Bool
  isNegative = (< zero)

type Subtr a = Subtr2 a a

class Subtr2 a b where
  subtr :: a -> b -> a
  default subtr :: (a ~ b,Num a) => a -> b -> a
  subtr = (-)

minMap :: (a -> a) -> Min a -> Min a
minMap f (Min (Just a)) = Min $ Just $ f a
minMap _ a              = a
sumMap :: (a -> a) -> Sum a -> Sum a
sumMap f (Sum (Just a)) = Sum $ Just $ f a
sumMap _ a              = a

instance Zero Int
instance Zero Integer
instance Zero Double
instance Zero Float
instance Pointed Sum where point = Sum . Just
instance Pointed Min where point = Min . Just
instance Zero v => Zero (Sum v) where
  zero = point zero
  isNegative (Sum Nothing)  = False
  isNegative (Sum (Just a)) = isNegative a
instance Zero v => Zero (Min v) where
  zero = point zero
  isNegative (Min Nothing)  = False
  isNegative (Min (Just a)) = isNegative a
