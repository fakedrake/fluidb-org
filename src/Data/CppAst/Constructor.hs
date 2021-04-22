{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveTraversable #-}

module Data.CppAst.Constructor
  ( Constructor(..)
  ) where

import           Data.CppAst.Argument
import           Data.CppAst.Expression
import           Data.CppAst.Statement
import           Data.CppAst.Symbol
import           Data.Utils.AShow
import           Data.Utils.Hashable
import qualified GHC.Generics           as GG

data Constructor a = Constructor {
  constructorMemberConstructors :: [(Symbol a, Expression a)],
  constructorBody               :: [Statement a],
  constructorArguments          :: [Argument a]
  } deriving (Eq, Show, Functor, Foldable, Traversable, GG.Generic)
instance Hashable a => Hashable (Constructor a)
instance AShow a => AShow (Constructor a)
