{-# LANGUAGE LambdaCase #-}
module Data.Query.Optimizations.Projections (squashProjections) where

import           Data.Query.Algebra


-- XXX: Squash
squashProjections :: (e -> e -> Bool) -> Query e s -> Query e s
squashProjections symEq = \case
  Q1 (QProj QProjNoInv pout) (Q1 (QProj QProjNoInv pin) q) -> recur
    $ Q1 (QProj QProjNoInv $ pout `squashMap` pin) q
  Q1 (QProj QProjInv {} _) _ ->
    error "Squashing projections called after exposing uniques."
  Q2 o l r -> Q2 o (recur l) (recur r)
  Q1 o q -> Q1 o (recur q)
  Q0 s -> Q0 s
  where
    recur = squashProjections symEq
    squashMap out inn = [(eo,expr >>= (`tryLookup` inn)) | (eo,expr) <- out]
      where
        tryLookup e m = case filter (symEq e . fst) m of
          []         -> E0 e -- Trust it's a literal
          (_,expr):_ -> expr
