{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.Assignment
  ( Assignment(..)
  ) where

import           Data.CppAst.Expression
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Symbol
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics


data Assignment a = PlusPlus (Symbol a)
                  | MinusMinus (Symbol a)
                  | Assignment (Symbol a) (Expression a)
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Assignment a)
instance AShow a => AShow (Assignment a)
instance (IsCode c, Codegen a c) => Codegen (Assignment a) c where
  toCode = \case
    PlusPlus s -> toCode s <> "++"
    MinusMinus s -> toCode s <> "--"
    Assignment s e -> toCode s <> " = " <> toCode e
