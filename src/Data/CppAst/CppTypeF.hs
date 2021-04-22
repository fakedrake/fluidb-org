{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.CppTypeF
  ( CppSize(..)
  , CppTypeF(..)
  ) where
import           Data.CppAst.LiteralCode.Codegen
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics                    (Generic)


-- |c is the surrounding code. Snippets of code are used in sizeof
-- parts of the type.
data CppTypeF c
  = CppInt
  | CppArray (CppTypeF c) (CppSize c)
  | CppDouble
  | CppChar
  | CppNat
  | CppVoid
  | CppBool
  deriving (Show,Eq,Functor,Foldable,Traversable,Generic)
instance Hashable a => Hashable (CppTypeF a)
instance AShow c => AShow (CppTypeF c)
instance ARead c => ARead (CppTypeF c)
data CppSize c = LiteralSize Int | SizeOf c
  deriving (Eq, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (CppSize a)
instance AShow a => AShow (CppSize a)
instance ARead a => ARead (CppSize a)
instance Show c => Show (CppSize c) where
  show (LiteralSize x) = "(LiteralSize "  ++ show x ++ ")"
  show (SizeOf _)      = "(SizeOf <some number>)"
instance (Codegen c' c, IsCode c) => Codegen (CppSize c') c where
  toCode (LiteralSize x) = toCode x
  toCode (SizeOf x)      = "sizeof(" <> toCode  x <> ")"

instance (IsCode c, Codegen c' c) => Codegen (CppTypeF c') c where
  toCode CppNat               = "unsigned"
  toCode CppBool              = "bool"
  toCode CppVoid              = "void"
  toCode CppInt               = "int"
  toCode CppChar              = "char"
  toCode CppDouble            = "double"
  toCode (CppArray CppChar n) = "fluidb_string<" <> toCode n <> ">"
  toCode (CppArray t n)       = "std::array<" <> toCode t
                                <> ", " <> toCode n <> ">"
