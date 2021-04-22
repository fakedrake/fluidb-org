{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveTraversable #-}

module Data.Utils.EmptyF (EmptyF(..),EmptyF2(..)) where

import           Data.Utils.Hashable
import           Data.Utils.AShow
import           GHC.Generics

data EmptyF a = EmptyF
  deriving (Functor, Traversable,Foldable,Generic,Eq,Show,Read)
instance Hashable (EmptyF a)
instance AShow (EmptyF a)
instance ARead (EmptyF a)
instance Monad EmptyF where
  (>>=) = const $ const EmptyF
instance Applicative EmptyF where
  pure = const EmptyF
  (<*>) = const $ const EmptyF
data EmptyF2 a b = EmptyF2 deriving (Functor,Traversable,Foldable,Generic,Eq,Show,Read)
instance Monad (EmptyF2 x) where
  (>>=) = const $ const EmptyF2
instance Applicative (EmptyF2 x) where
  pure = const EmptyF2
  (<*>) = const $ const EmptyF2
instance Hashable (EmptyF2 a b)
instance AShow (EmptyF2 a b)
instance ARead (EmptyF2 a b)
