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
  ,Subtr(..),sumMap,minMap) where

-- Use newtypes so they are coercible. Nothing means infinte
import           Data.Coerce
import           Data.Pointed
import           Data.Utils.AShow
import           GHC.Generics
newtype Sum a = Sum { unSum :: Maybe a } deriving (Show,Eq,Generic)
newtype Min a = Min { unMin :: Maybe a }
  deriving (Show,Eq,Generic)

newtype Neg a = Neg a
  deriving (Show,Eq,Generic)
pattern SumInf = Sum Nothing
pattern MinInf = Min Nothing

instance AShow a => AShow (Sum a)
instance AShow a => AShow (Min a)
instance AShow a => AShow (Neg a)

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
sub :: Subtr a => Sum a -> Neg a -> Sum a
sub SumInf _               = SumInf
sub (Sum (Just a)) (Neg b) = Sum $ Just $ subtr a b

class Zero a where
  zero  :: a
  default zero :: Num a => a
  zero = 0
class Subtr a where
  subtr :: a -> a -> a
  default subtr :: Num a => a -> a -> a
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
instance Zero v => Zero (Sum v) where zero = point zero
instance Zero v => Zero (Min v) where zero = point zero
