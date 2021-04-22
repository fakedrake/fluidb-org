{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies          #-}

module Data.Codegen.SchemaAssocClass
  ( SchemaAssoc
  , SchemaAssocClass(..)
  ) where

import           Data.String
import           Data.CppAst

class IsString e => SchemaAssocClass a e s where
  type SchemaAssoc' a e s
instance IsString e => SchemaAssocClass a e s where
  type SchemaAssoc' a e s = [(s, [(CppTypeF a, e)])]
type SchemaAssoc e s = SchemaAssoc' (Expression CodeSymbol) e s
