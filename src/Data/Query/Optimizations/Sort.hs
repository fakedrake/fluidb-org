{-# LANGUAGE LambdaCase #-}
module Data.Query.Optimizations.Sort (
  sortBeforeAggregating,optimizeSorts
  ) where

import           Data.Bifunctor
import           Data.List
import           Data.Query.Algebra
import           Data.Query.Optimizations.Types
import           Data.Utils.Functors

sortBeforeAggregating :: (e -> e -> Bool) -> Query e s -> Query e s
sortBeforeAggregating eqE = qmap' go where
  go = \case
    Q1 (QGroup proj grp@(_:_)) q ->
      -- XXX: IF GROUPING *REDEFINES* NAMES USING AGGREGATION
      -- EXPRESSIONS THIS WILL RESULT IN HAVING AGGREGATION FUNCTIONS
      -- IN SORT!! EITHER CHECK FOR THIS OR
      Q1 (QGroup proj grp) (Q1 (QSort $ replaceProj' eqE proj <$> grp) q)
    q -> q

replaceProj' :: (e -> e -> Bool) -> [(e, Expr (Aggr (Expr e)))] -> Expr e -> Expr e
replaceProj' eqE proj = fmap unWrapEq
  . replaceProj (fmap (bimap (WrapEq eqE) (fmap3 $ WrapEq eqE)) proj)
  . fmap (WrapEq eqE)

-- | Run this after joins are concrete.
optimizeSorts :: (e -> e -> Bool) -> Query e s -> Query e s
optimizeSorts eqE = qmap' rewrite where
  rewrite = \case
    S p (Q1 (QSort grp) q) -> Q1 (QSort grp) (S p q) -- Bottom up
    -- Check if the inner sort already sorts well enough
    Q1 (QSort srtOut) qg@(Q1 (QGroup proj _) (Q1 (QSort srtIn) _)) ->
      if null srtOutFiltered then qg else Q1 (QSort srtOutFiltered) qg
      where
        srtOutFiltered = fmap fst
          $ filter (not . (`elem'` srtIn) . snd)
          $ zip srtOut srtOut'
          where
            elem' e es = fmap (WrapEq eqE) e `elem` fmap2 (WrapEq eqE) es
            srtOut' = replaceProj' eqE proj <$> srtOut
    Q1 (QSort grp) (Q1 (QSort grp') q) ->
      Q1 (QSort (fmap2 unWrapEq $ nub $ fmap2 (WrapEq eqE) $ grp ++ grp')) q
    q -> q
