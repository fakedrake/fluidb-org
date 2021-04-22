{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Query.Optimizations.Misc
  ( fixSubstringIndexOffset
  ) where

import           Data.Functor.Identity
import           Data.Query.Algebra

-- | index offsets are 1 based in sql syntax. Make them 0 based.
fixSubstringIndexOffset :: Query e s -> Query e s
fixSubstringIndexOffset = (runIdentity .) $ exprLens $ \case
  E1 (EFun (SubSeq from to)) e ->
    return $ E1 (EFun (SubSeq (from - 1) (to - 1))) e
  e -> pure e
