{-# LANGUAGE LambdaCase #-}
module Data.Query.Optimizations.Utils (expTypeSymCppType) where

import           Data.CppAst.CppType
import           Data.Query.SQL.Types

expTypeSymCppType :: ExpTypeSym' e -> Maybe (CppTypeF x)
expTypeSymCppType = \case
  EDate _ -> Just CppNat
  EInterval _ -> Just CppInt
  EFloat _ -> Just CppDouble
  EInt _ -> Just CppInt
  EString x -> Just $ CppArray CppChar $ LiteralSize $ length x
  ESym _ -> Nothing
  EBool _ -> Just CppBool
  ECount _ -> Just CppInt
