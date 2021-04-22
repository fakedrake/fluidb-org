{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Data.CppAst.Symbol (
  Symbol(..)
                                                  )where

import           Data.CppAst.LiteralCode.Codegen
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

newtype Symbol a = Symbol {runSymbol :: a}
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Symbol a)
instance AShow a => AShow (Symbol a)
instance ARead a => ARead (Symbol a)
instance IsString a => IsString (Symbol a) where
  fromString = Symbol . fromString
instance (IsCode c, Codegen a c) => Codegen (Symbol a) c where
  toCode = toCode . runSymbol
