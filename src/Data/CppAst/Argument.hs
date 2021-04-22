{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.Argument (
  Argument(..)) where

import           Data.CppAst.Declaration
import           Data.CppAst.LiteralCode.Codegen
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

newtype Argument a = Argument {runArgument :: Declaration a}
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Argument a)
instance AShow a => AShow (Argument a)
instance Codegen a c => Codegen (Argument a) c where
  toCode = toCode . runArgument
