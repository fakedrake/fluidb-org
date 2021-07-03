{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Data.Query.Optimizations.ExposeUnique (exposeUnique,disambEq) where

import           Control.Monad.Except
import           Data.Bifunctor
import           Data.Either
import           Data.List
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Utils.Functors


-- | When there is an equality between unique keys we don't need to
-- keep both of them:
--
-- > disambEq (foldr1Unsafe And $ [P0 (R2 REq (R0 (E0 l)) (R0 (E0 r))) | (r,l) <- [(1,2),(2,3)]]) [1,2,3,4]
-- [[1,4],[2,4],[3,4]]
disambEq :: forall e . Eq e =>
           Prop (Rel (Expr e))
         -> [e] -> [[e]]
disambEq p uniqs0 = sequence uniqsPrim
  where
    -- eqGrps are groups of symbols that are equal to each
    -- other. These groups are obviously non-empty and disjoint
    eqGrps = go eqs
      where
        overlaps x y = any (`elem` y) x
        go [] = []
        go (e:es) = nub (join $ e:grp):go rst where
          (grp,rst) = span (overlaps e) es
        eqs = toList (propQnfAnd p) >>= \case
          P0 (R2 REq (R0 (E0 x)) (R0 (E0 y))) -> [[x,y] | x /= y]
          _                                   -> []
    -- instead of each element of the uniqs group we have a list of
    -- equivalent symbols
    uniqsPrim = nub [maybe [u] (\case {[] -> undefined;xs -> xs})
                     $ find (elem u) eqGrps
                    | u <- uniqs0]

exposeUnique :: (MonadPlus m, Eq e) =>
               (e -> m e) -> Query e (s,[e]) -> m (Query e s,[[e]])
exposeUnique mkUniq = recQ (exposeUnique0 mkUniq)
  . fmap (return . bimap Q0 return)
exposeUnique0 :: (Eq e, MonadPlus m) =>
                (e -> m e)
              -> Query0 e (m (Query e s,[[e]]))
              -> m (Query e s,[[e]])
exposeUnique0 uniqSym = \case
  Left (bop,l,r) -> exposeUniqueB1 bop <$> l <*> r
  Right (uop,q)  -> exposeUniqueU1 uniqSym uop =<< q
exposeUniqueB1 :: Eq e =>
                 BQOp e
               -> (Query e s,[[e]])
               -> (Query e s,[[e]])
               -> (Query e s,[[e]])
exposeUniqueB1 o (ql,esl) (qr,esr) = (Q2 o ql qr,) $ case o of
  -- Note: n case of a join that is a foreign key join, we have the
  -- opportunity to drop one completely. This is implemented in the
  -- case of QueryPlan `joinPlan`. TODO: Use QueryPlans here too.
  QJoin p          -> (++) <$> esl <*> esr >>= disambEq p
  QProd            -> (++) <$> esl <*> esr
  QUnion           -> esl
  QDistinct        -> esl -- We will be grouping by these so they will be uniq
  QProjQuery       -> error "We can't infer unique set for QProjQuery"
  QLeftAntijoin _  -> esl
  QRightAntijoin _ -> esr

-- Count exposed symbols.
exposeUniqueU1 :: forall e s m . (MonadPlus m,Eq e) =>
                 (e -> m e)
               -> UQOp e
               -> (Query e s,[[e]])
               -> m (Query e s,[[e]])
exposeUniqueU1 uniqName o (q,allPrimss) = case o of
  QProj p ->
    putPrj (\p' -> Q1 (QProj (p ++ p')) q) E0 (mapMaybe extractProjRemap p) allPrimss
  QGroup p [] -> return (Q1 (QGroup p []) q,return . fst <$> p)
  QGroup p g -> do
    let primss = case traverse (\case{E0 e -> Just e;_ -> Nothing}) g of
          Nothing -> allPrimss
          Just es -> es:allPrimss
    putPrj
      (\p' -> Q1 (QGroup (p ++ p') g) q)
      (E0 . NAggr AggrFirst . E0)
      (mapMaybe extractAggrRemap p)
      primss
  -- Note: see join for explanation o disambEq (there may be a join
  -- hidden in the selection)
  QSel p -> pure (Q1 o q,disambEq p =<< allPrimss)
  QSort _ -> pure (Q1 o q,allPrimss)
  QDrop _ -> pure (Q1 o q,allPrimss)
  QLimit _ -> pure (Q1 o q,allPrimss)
  where
    putPrj :: ([(e,a)] -> Query e s)
           -> (e -> a)
           -> [(e,e)]
           -> [[e]]
           -> m (Query e s,[[e]])
    putPrj qWithUniqs toExp exposeMap primss = do
      -- Either (the provided key) (the exposed counterpart)
      let exported primKey = case primKey `lookup` exposeMap of
            Nothing -> Left primKey
            Just x  -> Right x
      -- Get entirely exposed primary sets and unexposed/exposed partitionings
      (expPrimss,unexpMaps) <- fmap partitionEithers $ forM primss $ \prims ->
        case partitionEithers $ exported <$> prims of
          ([],exps) -> return $ Left exps
          (unexpKeys,expKeys) -> Right . (,expKeys)
            <$> traverse (\k -> (,toExp k) <$> uniqName k) unexpKeys
      -- If we are exporting any sets entirely, don't bother with the syms
      case expPrimss of
        _:_ -> return (qWithUniqs [],expPrimss)
        [] -> msum $ unexpMaps <&> \(unexpMap,expSyms) ->
          return (qWithUniqs unexpMap,[expSyms ++ map fst unexpMap])

extractAggrRemap :: (e, Expr (Aggr (Expr e0))) -> Maybe (e0, e)
extractAggrRemap = \case
  (e,E0 (NAggr AggrFirst (E0 ei))) -> Just (ei,e)
  _                                -> Nothing
extractProjRemap :: (e, Expr e0) -> Maybe (e0, e)
extractProjRemap = \case
  (e,E0 ei) -> Just (ei,e)
  _         -> Nothing

type Query0 e a = Either (BQOp e, a, a) (UQOp e, a)

-- | Fix projections/groupings that do not expose all the unique
-- columns.
recQ :: (Query0 e a -> a) -> Query e a -> a
recQ f = \case
  Q0 a      -> a
  Q1 o q    -> f $ Right $ (o, recQ f q)
  Q2 o q q' -> f $ Left $ (o, recQ f q, recQ f q')
