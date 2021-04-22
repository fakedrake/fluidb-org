{-# LANGUAGE ConstraintKinds #-}
module Data.Utils.Hashable
  (Hashables1
  ,Hashables2
  ,Hashables3
  ,Hashables4
  ,module Data.Hashable) where

import           Data.Hashable
type Hashables1 n = (Eq n, Hashable n)
type Hashables2 a b = (Hashables1 a, Hashables1 b)
type Hashables3 a b c = (Hashables1 a, Hashables2 b c)
type Hashables4 a b c d = (Hashables2 a b, Hashables2 c d)
