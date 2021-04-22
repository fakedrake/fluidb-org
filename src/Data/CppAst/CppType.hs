module Data.CppAst.CppType (CppType,module Data.CppAst.CppTypeF) where

import           Data.CppAst.CodeSymbol
import           Data.CppAst.CppTypeF
import           Data.CppAst.Expression

type CppType = CppTypeF (Expression CodeSymbol)
