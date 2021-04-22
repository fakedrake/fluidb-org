{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Data.CppAst.Operator
  ( Operator(..)
  ) where


import           Data.CppAst.LiteralCode.Codegen
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

newtype Operator = Operator {runOperator :: String}
  deriving (Eq, Show, Generic)
instance Hashable Operator
instance AShow Operator
instance ARead Operator
instance IsString Operator where
  fromString = Operator . fromString
instance IsCode c => Codegen Operator c where
  toCode = fromString . runOperator
