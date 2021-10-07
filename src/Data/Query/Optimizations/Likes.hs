{-# LANGUAGE CPP                 #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Query.Optimizations.Likes
  ( likesToEquals
  , equalsToLikes
  ) where

import           Data.Functor.Identity
import           Data.List
import           Data.Query.Algebra
import           Data.Query.Optimizations.Types
import           Data.Query.SQL.Types
import           Data.Utils.Functors

-- | a like '%1234' -> suffix<4>(a) = '1234' to allow for equijoins.
likesToEquals :: forall e' e s .
                SymEmbedding (ExpTypeSym' e) s e'
              -> Query e' s -> Query e' s
likesToEquals SymEmbedding{..} q =
  runIdentity $ traverseProp (Identity . fmap (qmap go)) q
  where
    cotail :: [a] -> [a]
    cotail []     = []
    cotail [_]    = []
    cotail (x:xs) = x:cotail xs
    go :: Rel (Expr e') -> Rel (Expr e')
    go r@(R2 RLike (R0 rl) (R0 (E0 strPat))) = case unEmbed strPat of
      EString pat ->
        if | "%" `isPrefixOf` pat -> eqPattern Suffix r rl $ tail pat
           | "%" `isSuffixOf` pat -> eqPattern Prefix r rl $ cotail pat
           | '%' `elem` pat       -> r
           | otherwise            -> eqPattern AssertLength r rl pat
      _ -> r
    go x = x
    eqPattern :: (Int -> ElemFunction)
              -> Rel (Expr e')
              -> Expr e'
              -> String
              -> Rel (Expr e')
    eqPattern func r rl pat =
      if | '%' `elem` pat -> r
         | null pat -> R0 $ E0 $ embedLit $ EBool True
         | otherwise ->
           (R2 REq
             (R0 $ E1 (EFun $ func $ length pat) rl)
             (R0 $ E0 $ embedLit $ EString pat))

-- | C++ string equality is a bit too strict as it expects that the
-- types are also equal (ie the size) so we will downgrade all
-- equalities to likes. This is not 100% correct as we would be
-- changing. `x = "hello%"` to `x like "hello%"` but it's fine for
-- TPC-H.
equalsToLikes
  :: forall e s . (e -> Bool) -> (e -> Bool) -> Query e s -> Query e s
equalsToLikes isLit isString = runIdentity . traverseProp
  (Identity . fmap replaceRel . fmap2 replaceExpr)
  where
    replaceExpr :: Expr e -> Expr e
    replaceExpr = \case
      E2 EEq (E0 x) y  -> mkLike x y
      E2 EEq x (E0 y)  -> mkLike y x
      E2 ENEq (E0 x) y -> E1 ENot $ mkLike x y
      E2 ENEq x (E0 y) -> E1 ENot $ mkLike y x
      E2 o l r         -> E2 o (replaceExpr l) (replaceExpr r)
      E1 o l           -> E1 o $ replaceExpr l
      E0 s             -> E0 s
      where
        mkLike x y = if all isLit y && all isString y && not (isLit x)
                     then E2 ELike (E0 x) y
                     else E2 EEq (E0 x) $ replaceExpr y
    replaceRel :: Rel (Expr e) -> Rel (Expr e)
    replaceRel = \case
      R2 REq (R0 (E0 x)) y -> mkLike x y
      R2 REq x (R0 (E0 y)) -> mkLike y x
      R2 o l r             -> R2 o (replaceRel l) (replaceRel r)
      R0 s                 -> R0 s
      where
        mkLike x y@(R0 _) = if all2 isLit y && all2 isString y && not (isLit x)
          then R2 RLike (R0 (E0 x)) y
          else R2 REq (R0 (E0 x)) $ replaceRel y
        mkLike x y = R2 REq (R0 (E0 x)) $ replaceRel y
