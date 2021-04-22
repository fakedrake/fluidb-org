{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Query.Optimizations.SanityCheck () where

#if 0
sanityCheck :: forall s t n m .
              MonadOpt ExpTypeSym s t n m =>
              Query ExpTypeSym s
            -> ExceptT (SanityError ExpTypeSym s) m ()
sanityCheck q = do
  -- This is kind of checked in typeEq too but typeEq won't detect
  -- ambiguous variables.
  variableScopeCheck q
  relationTypeCheck typeEq q
  where
    typeEq :: [QueryPlan ExpTypeSym s]
           -> Expr ExpTypeSym
           -> Expr ExpTypeSym
           -> ExceptT
           (SanityError ExpTypeSym s) m ()
    typeEq qs l r = do
      Tup2 lType rType  <- lift
        $ (runSoftCodeBuilder . exprCppType qs) `traverse` Tup2 l r
      if lType `compatibleWith` rType
        then return ()
        else throwE $ UnequalTypes (lType, l) (rType, r)
    compatibleWith t1 t2 = typeCode t1 == typeCode t2 && case (t1, t2) of
      (CppArray CppChar _, CppArray CppChar _) -> True
      (CppArray t1' s1, CppArray t2' s2) ->
        compatibleWith t1' t2' && case (s1, s2) of
                                   (LiteralSize ls, LiteralSize lr) -> ls == lr
                                   (_, _) -> True
      _ -> True
    typeCode :: CppType -> Integer
    typeCode x = case x of
      CppInt       -> 0
      CppDouble    -> 0
      CppNat       -> 0
      CppChar      -> 1
      CppBool      -> 2
      CppVoid      -> 3
      CppArray _ _ -> 4


-- | Check that all expressions uniquely refer to a single
-- variables. Ie there is no name shadowing. Nothing means e is a
-- literal.
variableScopeCheck :: forall e s t n m .
                     (MonadOpt e s t n m) =>
                     Query e s -> ExceptT (SanityError e s) m ()
variableScopeCheck = void . qmapM' go where
  go q = (>> return q) $ case q of
    J p l r -> mapM_ (`assertRefToExactlyOne` [l, r]) (PTrav p)
    S p l   -> mapM_ (`assertRefToExactlyOne` [l]) (PTrav p)
    _       -> return ()
    where
      assertRefToExactlyOne e ss = maybe (return ()) throwE =<< refersToExactlyOne


relationTypeCheck :: MonadOpt e s t n m =>
                    ([Query e s] -> Expr e -> Expr e ->
                     ExceptT (SanityError e s) m ())
                  -> Query e s
                  -> ExceptT (SanityError e s) m ()
relationTypeCheck typeEq = void . traverseProp' (traverse . check)
  where
    check qs = \case
      q@(R2 _ (R0 l) (R0 r)) -> typeEq qs l r >> return q
      q -> return q
#endif
