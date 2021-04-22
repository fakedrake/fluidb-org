{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveTraversable #-}

module Data.CppAst.TmplDefArg
  ( TmplDefArg(..)
  ) where

import           Data.CppAst.Expression
import           Data.CppAst.Symbol
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

data TmplDefArg a = TypeArgDef (Symbol a) | TmplArgDef (Type a) (Symbol a)
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (TmplDefArg a)
instance AShow a => AShow (TmplDefArg a)
