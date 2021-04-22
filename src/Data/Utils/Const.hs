{-# LANGUAGE DeriveFoldable      #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Utils.Const (Const(..),CoConst(..)) where

import           Data.Bifoldable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Utils.Hashable
import           Data.Void
import           GHC.Generics


-- | hash (Const x) == hash (Left x) and hash (CoConst x) == hash (Right x)

newtype Const a b = Const { getConst :: a }
  deriving (Functor, Traversable, Foldable, Show, Generic, Read, Eq)

instance Bifunctor Const where bimap g _ = Const . g . getConst
instance Bifoldable Const where
  bifoldr f _ i = flip f i . getConst
instance Bitraversable Const where
  bitraverse g _ = fmap Const . g . getConst
instance Hashable a => Hashable (Const a b) where
  hashWithSalt s (Const a) = hashWithSalt s $ (Left a :: Either a Void)
  {-# INLINE hashWithSalt #-}
  hash (Const a) = hash (Left a :: Either a Void)
  {-# INLINE hash #-}
newtype CoConst a b = CoConst { getCoConst :: b }
  deriving (Functor, Traversable, Foldable, Show, Generic, Read, Eq)
instance Bifunctor CoConst where bimap _ g = CoConst . g . getCoConst
instance Bifoldable CoConst where
  bifoldr _ f i = flip f i . getCoConst
instance Bitraversable CoConst where
  bitraverse _ g = fmap CoConst . g . getCoConst
instance Hashable b => Hashable (CoConst a b) where
  hashWithSalt s (CoConst a) = hashWithSalt s $ (Right a :: Either Void b)
  {-# INLINE hashWithSalt #-}
  hash (CoConst a) = hash $ (Right a :: Either Void b)
  {-# INLINE hash #-}
