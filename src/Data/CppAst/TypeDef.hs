{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.TypeDef
  ( TypeDef(..)
  ) where
import           Data.CppAst.Expression
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Symbol
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics                    (Generic)

data TypeDef a = TypeDef (Type a) (Symbol a)
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (TypeDef a)
instance AShow a => AShow (TypeDef a)
instance (IsCode c, Codegen a c) => CodegenIndent (TypeDef a) c where
  toCodeIndent' (TypeDef ty sym) =
    putLine $ "typedef " <> toCode ty <> " " <> toCode sym <> ";"
