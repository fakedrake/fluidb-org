{-# LANGUAGE TypeFamilies #-}
module Data.Utils.OptSet
  (OptSet) where

import qualified Data.IntSet as IS
-- | Optimized set
type family OptSet i :: *
type instance OptSet Int = IS.IntSet
