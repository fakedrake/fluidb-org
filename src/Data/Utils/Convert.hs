{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeOperators #-}
module Data.Utils.Convert
  (Convertible(..)) where

import GHC.Generics

class Convertible a b where
    convert :: a -> b
    default convert :: (Generic a, Generic b, GConvertible (Rep a) (Rep b))
                    => a -> b
    convert = to . gconvert . from

class GConvertible f g where
    gconvert :: f a -> g a

instance GConvertible V1 V1 where
    gconvert = id

instance GConvertible U1 U1 where
    gconvert = id

instance Convertible c d => GConvertible (K1 i c) (K1 j d) where
    gconvert = K1 . convert . unK1

instance GConvertible f g => GConvertible (M1 i c f) (M1 j d g) where
    gconvert = M1 . gconvert . unM1

instance (GConvertible f1 f2, GConvertible g1 g2)
    => GConvertible (f1 :+: g1) (f2 :+: g2) where
    gconvert (L1 l) = L1 (gconvert l)
    gconvert (R1 r) = R1 (gconvert r)

instance (GConvertible f1 f2, GConvertible g1 g2)
    => GConvertible (f1 :*: g1) (f2 :*: g2) where
    gconvert (l :*: r) = gconvert l :*: gconvert r
