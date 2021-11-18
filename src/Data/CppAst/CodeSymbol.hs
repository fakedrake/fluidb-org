{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Data.CppAst.CodeSymbol
  ( CodeSymbol(..)
  , symbolRef
  ) where

import           Data.Char
import           Data.CppAst.LiteralCode.Codegen
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

data CodeSymbol
  = UniqueSymbolDef String Int
  | UniqueSymbolRef String Int
  | CppLiteralSymbol String
  | CppSymbol String
  deriving (Eq,Generic,Show)
symbolRef :: CodeSymbol -> CodeSymbol
symbolRef (UniqueSymbolDef s i) = UniqueSymbolRef s i
symbolRef x                     = x
instance Hashable CodeSymbol
instance AShow CodeSymbol
instance ARead CodeSymbol
instance IsString CodeSymbol where
  fromString = CppLiteralSymbol
instance IsCode c => Codegen CodeSymbol c where
  toCode = \case
    UniqueSymbolDef s i -> fromString $ s ++ show i
    UniqueSymbolRef s i -> fromString $ s ++ show i
    CppLiteralSymbol s -> fromString s
    CppSymbol s -> fromString $ norm =<< s
      where
        norm c | isAlphaNum c = [c]
               | c == '_' = "__"
               | otherwise = "_" <> show (ord c) <> "_"
