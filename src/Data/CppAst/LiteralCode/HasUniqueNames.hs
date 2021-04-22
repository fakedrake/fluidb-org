module Data.CppAst.LiteralCode.HasUniqueNames
  ( HasUniqueNames(..)
  ) where

class HasUniqueNames c where
  maxNameIndex :: c -> Int
