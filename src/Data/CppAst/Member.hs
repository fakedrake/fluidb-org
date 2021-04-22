{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.Member
  ( Member(..)
  ) where
import           Data.CppAst.Declaration
import           Data.CppAst.Function
import           Data.CppAst.LiteralCode.Codegen
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics                    (Generic)

data Member a = MemberFunction (Function a) | MemberVariable (Declaration a)
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Member a)
instance AShow a => AShow (Member a)
instance Codegen a c => CodegenIndent (Member a) c where
  toCodeIndent' = \case
    MemberVariable v -> toCodeIndent' v
    MemberFunction f -> toCodeIndent' f
