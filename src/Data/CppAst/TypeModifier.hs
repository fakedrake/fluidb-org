{-# LANGUAGE DeriveGeneric #-}
module Data.CppAst.TypeModifier
  ( TypeModifier(..)
  ) where

import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics


data TypeModifier = Pointer | Reference | CppConst | Static
  deriving (Ord, Eq, Show, Generic)
instance Hashable TypeModifier
instance AShow TypeModifier
instance ARead TypeModifier
