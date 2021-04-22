{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
module Data.CppAst.Declaration
  ( Declaration(..)
  , declarationNameRef
  ) where

import           Data.CppAst.CodeSymbol
import           Data.CppAst.Expression
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Symbol
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

data Declaration a = Declaration {
  declarationName :: Symbol a,
  declarationType :: Type a
  } deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
declarationNameRef :: Declaration CodeSymbol -> Symbol CodeSymbol
declarationNameRef = fmap symbolRef . declarationName
instance AShow a => AShow (Declaration a)
instance Hashable a => Hashable (Declaration a)
instance Codegen a c => Codegen (Declaration a) c where
  toCode Declaration{..} = toCode declarationType
                           <> " "
                           <> toCode declarationName

instance Codegen a c => CodegenIndent (Declaration a) c where
  toCodeIndent' x = putLine $ toCode x <> ";"
