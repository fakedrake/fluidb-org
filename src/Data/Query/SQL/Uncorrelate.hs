{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE LiberalTypeSynonyms   #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE ViewPatterns          #-}
{-# OPTIONS_GHC -Wno-unused-binds -Wno-name-shadowing #-}

module Data.Query.SQL.Uncorrelate (UnnestError,unnestQuery) where

import           Control.Monad
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           Control.Monad.Trans.Maybe
import           Control.Monad.Writer       hiding (Endo)
import           Data.Either
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Types
import           Data.Utils.Unsafe
import           GHC.Base                   (Alternative (..))
import           GHC.Generics

data UnnestError s e =
  UnnestAStrError (AShowStr e s)
  | NoNonfreeVarsInCorrel (Maybe (Prop (Rel (Expr e))))
  | ExpectedGrouping (Rel (Expr e)) (Query e s)
  deriving (Eq, Generic)
instance AShowError e s (UnnestError s e)
instance (AShowV e,AShowV s) => AShow (UnnestError e s)
type UnnestMonad e s = ReaderT (e -> Query e s -> Maybe Bool) (StateT Int (Either (UnnestError s e)))
data CorrelProp e = CorrelProp {
  -- The part of the proposition that has no free variables
  -- p0
  correlPropNonFree :: Maybe (Prop (Rel (Expr e))),
  -- The part of the proposition that has free variables
  -- pi
  correlPropFree    :: [([e], Prop (Rel (Expr e)))]}
  deriving Show
emptyCorrelProp :: CorrelProp e
emptyCorrelProp = CorrelProp Nothing []

-- | Given a way to distingwish between free and non-free variables
-- make the CorrelProp. This only works for props
mkCorrelProp :: (e -> Bool) -> Prop (Rel (Expr e)) -> CorrelProp e
mkCorrelProp isFreeVar p = CorrelProp{..} where
  (correlPropNonFree, fmap2 P0 -> correlPropFree) =
    runWriter $ runMaybeT $ extractRelIf isFreeVar p

data ExistentialCorrelRecord e s = ExistentialCorrelRecord {
  -- p
  existCorrelSel     :: CorrelProp e,
  -- B
  existCorrelQuery   :: Query e (CorrelHead e s),
  -- A
  existCorrelSubTree :: Query e (CorrelHead e s)
  }
data GroupCorrelRecord e s = GroupCorrelRecord {
  grpCorrelQueryVal :: Expr (Aggr (Expr e)),
  -- p
  grpCorrelSel      :: CorrelProp e,
  -- R
  grpCorrelProp     :: e -> UnnestMonad e s (Prop (Rel (Expr e))),
  -- B
  grpCorrelQuery    :: Query e (CorrelHead e s),
  -- A
  grpCorrelSubTree  :: Query e (CorrelHead e s)
  }

data CorrelHead e s = NoCorrel s
                    | ExistentialCorrel Bool (ExistentialCorrelRecord e s)
                    | GroupCorrel (GroupCorrelRecord e s)

-- | Normalize the query so that @uncorrelInterm@ can act directly on
-- selections.
normalize :: Query (NestedQueryE s e) s -> Query (NestedQueryE s e) s
normalize = qmap $ \case
  J p a b -> S p (Q2 QProd a b)
  S p a   -> S p a
  q       -> q
  where
    -- We can only deal with one
    -- breakSel = S
    breakSel p q = ($ q) $ foldr (.) id $ fmap S $ foldP $ span isCorr $ breakP p
      where
        foldP (correls,[])          = correls
        foldP ([correlP],noCorrels) = [foldl And correlP noCorrels]
        foldP (correls,noCorrels)   = foldr1Unsafe And noCorrels:correls
        isCorr = any (isLeft . runNestedQueryE) . toList3
        breakP = \case
          And l r -> breakP l ++ breakP r
          x       -> [x]

-- | Extract and-CNF terms of the form (P0 r) where exists e in r . f
-- e. Useful for extracting exists terms.
extractRelIf :: (e -> Bool)
              -> Prop (Rel (Expr e))
              -> MaybeT (Writer [([e], Rel (Expr e))]) (Prop (Rel (Expr e)))
extractRelIf f = extractRelIfM (return . f)

extractRelIfM :: forall e m . Monad m =>
                (e -> m Bool)
              -> Prop (Rel (Expr e))
              -> MaybeT (WriterT [([e], Rel (Expr e))] m) (Prop (Rel (Expr e)))
extractRelIfM shouldExtract p = MaybeT $ extractTerms go p where
  go :: Prop (Rel (Expr e)) -> m (Maybe ([e], Rel (Expr e)))
  go = \case
    P0 r -> do
      es <- filterM shouldExtract $ toList2 r
      return $ if null es then Nothing else Just (es, r)
    _ -> return Nothing

extractTerms :: forall r a m . Monad m =>
               (Prop r -> m (Maybe a))
             -> Prop r
             -> WriterT [a] m (Maybe (Prop r))
extractTerms shouldExtr = go where
  go :: Prop r -> WriterT [a] m (Maybe (Prop r))
  go = \case
    And p p' -> do
      p0 <- go p
      p0' <- go p'
      case (p0, p0') of
        (Just x, Just y) -> return2 $ And x y
        _                -> return $ p0 <|> p0'
    p -> do
      valM <- lift $ shouldExtr p
      case valM of
        Just val -> tell [val] >> return Nothing
        Nothing  -> return $ Just p

hasRelM :: Monad m => (e -> m Bool) -> Rel (Expr e) -> m Bool
hasRelM relTest = fmap (all2 id) . traverse2 relTest

-- | Remove intermediates: used to find the types as correlation heads
-- represent selections and do not affect the schema.
removeInterm :: Query e (CorrelHead e s) -> Query e s
removeInterm q = q >>=  \case
  NoCorrel s -> Q0 s
  ExistentialCorrel _ ExistentialCorrelRecord{..} ->
    removeInterm existCorrelSubTree
  GroupCorrel GroupCorrelRecord{..} -> removeInterm grpCorrelSubTree

-- | Match the axiom patterns into CorrelHead constructors so that we
-- can then rewrite them.
uncorrelInterm :: forall s e . (Eq e,Symbolic e,SymbolType e ~ String) =>
                 Query (NestedQueryE s e) s
               -> UnnestMonad e s (Query e (CorrelHead e s))
uncorrelInterm = \case
  Q2 o a b -> Q2 <$> coerceOp o <*> recur a <*> recur b
  S p q0 -> mkCorrelHead (p,q0) >>= \case
    Nothing -> case traverse3 coerceESym p of
      Right o' -> S o' <$> recur q0
      Left _ -> throwAStr $ "Neither a valid correl nor non-correl: " ++ ashow p
    Just s -> return $ Q0 s
  Q1 o a -> Q1 <$> coerceOp o <*> recur a
  Q0 s -> return $ Q0 $ NoCorrel s
  where
    recur = uncorrelInterm
    coerceOp :: forall op . (HasCallStack,Traversable op,(forall x . AShow x => AShow (op x))) =>
               op (NestedQueryE s e) -> UnnestMonad e s  (op e)
    coerceOp o = case traverse coerceESym o of
      Left _  -> throwAStr $ "Only QSel can contain correl ops: " ++ ashow o
      Right x -> return x
-- | From a proposition that contains relations
data CorrelRelation e s = CorrelRelation {
  correlRelationQuery :: Query e (CorrelHead e s),
  correlRelationProp  :: e -> UnnestMonad e s (Rel (Expr e))
  } deriving Generic
mkCorrelRelation :: (Eq e,Symbolic e,SymbolType e ~ String) =>
                   Prop (Rel (Expr (NestedQueryE s e)))
                 -> UnnestMonad e s (Maybe (CorrelRelation e s))
mkCorrelRelation (P0 r) = case filter isQuery $ moduloOne2 r of
  [(p, NestedQueryE (Left q))] -> do
    correlRelationQuery <- uncorrelInterm q
    let correlRelationProp = traverse2 coerceESym . p . NestedQueryE . Right
    return $ Just CorrelRelation{..}
  [] -> return Nothing
  _ -> throwAStr "Unsupported: multiple correlated queries per relation."
  where
    isQuery (_, NestedQueryE (Left _)) = True
    isQuery _                          = False
mkCorrelRelation _ = return Nothing

-- | Split a propsition into a variable proposition and a set of
-- relations with queries.
splitRelations :: (Eq e,Symbolic e,SymbolType e ~ String) =>
                 Prop (Rel (Expr (NestedQueryE s e)))
               -> UnnestMonad e s
                 ([CorrelRelation e s], Maybe (Prop (Rel (Expr (NestedQueryE s e)))))
splitRelations p = do
  (normalProp, correlProps) <- runWriterT $ extractTerms mkCorrelRelation p
  return (correlProps, normalProp)
  where
    isCorrelProp :: NestedQueryE s e -> Identity Bool
    isCorrelProp = return . isLeft . runNestedQueryE


mkCorrelHead :: (Eq e,Symbolic e,SymbolType e ~ String) =>
               (Prop (Rel (Expr (NestedQueryE s e))),
                Query (NestedQueryE s e) s)
             -> UnnestMonad e s (Maybe (CorrelHead e s))
mkCorrelHead (p,restQ) =
  if all3 (isRight . runNestedQueryE) p
  then return Nothing
  else getTheFirst [
    mkGrpCorrel (p,restQ),
    mkExistCorrel True (p,restQ),
    mkExistCorrel False (p,restQ)]
  where
    getTheFirst :: [UnnestMonad e s (Maybe a)]
                -> UnnestMonad e s (Maybe a)
    getTheFirst [] = return Nothing
    getTheFirst (x:xs) = x >>= \case
      Nothing -> getTheFirst xs
      x0      -> return x0

-- | Given B, extract p and B0 where B ~ g (sel (p, B0)) where p is
-- correlated.
extractEmptyGrp :: (Eq e, Symbolic e,SymbolType e ~ String) =>
                  Rel (Expr e)
                -> Query e (CorrelHead e s)
                -> UnnestMonad e s
                  ((Query e (CorrelHead e s), CorrelProp e),
                   Expr (Aggr (Expr e)))
extractEmptyGrp r = \case
  Q1 (QGroup [(_,grp)] []) q -> (,grp) <$> extractSelection q
  q -> do
    q' <- uncorrelFinal q
    throwError $ ExpectedGrouping r q'

extractSelection :: Query e (CorrelHead e s)
                 -> UnnestMonad e s (Query e (CorrelHead e s), CorrelProp e)
extractSelection = \case
  -- XXX: Here we assume that all free variables are in p, but there
  -- may be some in b0 too.
  S p b0 -> do
    inQ <- ask
    let isFreeVar e = (== Just True) $ fmap not $ e `inQ` removeInterm b0
    return (b0, mkCorrelProp isFreeVar p)
  q -> return (q, emptyCorrelProp)


-- | Non-existence
mkGrpCorrel :: (Eq e, Symbolic e,SymbolType e ~ String) =>
              (Prop (Rel (Expr (NestedQueryE s e))),
               Query (NestedQueryE s e) s)
            -> UnnestMonad e s (Maybe (CorrelHead e s))
mkGrpCorrel (rB, innerQ) = do
  (rs, innerP) :: ([CorrelRelation e s],
                  Maybe (Prop (Rel (Expr (NestedQueryE s e))))) <-
    splitRelations rB
  case rs of
    [CorrelRelation{..}] -> do
      innerQUncorr <- uncorrelInterm $ case innerP of
        Nothing -> innerQ
        Just p  -> S p innerQ
      correlRelationProp (mkSymbol "<subquery>") >>= \case
        r@R2{} -> do
          ((b0, p), grp) <- extractEmptyGrp r correlRelationQuery
          return2 $ GroupCorrel GroupCorrelRecord {
            grpCorrelQueryVal=grp,
            -- p
            grpCorrelSel=p,
            -- R
            grpCorrelProp=fmap P0 . correlRelationProp,
            -- B0
            grpCorrelQuery=b0,
            -- A
            grpCorrelSubTree=innerQUncorr
            }
        _ -> return Nothing
    [] -> return Nothing
    -- XXX: Maybe if we have more than one groupings.
    _ -> return Nothing

mkExistCorrel :: forall s e . (Eq e,Symbolic e,SymbolType e ~ String) =>
                Bool
              -> (Prop (Rel (Expr (NestedQueryE s e))),
                 Query (NestedQueryE s e) s)
              -> UnnestMonad e s (Maybe (CorrelHead e s))
mkExistCorrel doesExist (prop,a') = do
  (innerP,existQueries) <- runWriterT
    $ extractTerms (extractQuery uncorrelInterm doesExist) prop
  case existQueries of
    [] -> return Nothing
    [b] -> do
      a <- uncorrelInterm $ case innerP of {Nothing -> a' ; Just p -> S p a'}
      (b0, p) <- extractSelection b
      return2 $ ExistentialCorrel doesExist ExistentialCorrelRecord {
        -- p
        existCorrelSel=p,
        -- B
        existCorrelQuery=b0,
        -- A
        existCorrelSubTree=a}
    _:_ -> throwAStr $ "too many existential terms (only one allowed): "
          ++ ashow prop

extractQuery :: Monad m =>
               (Query (NestedQueryE s e) s -> m q)
             -> Bool -> Prop (Rel (Expr (NestedQueryE s e)))
             -> m (Maybe q)
extractQuery uncorrel doesExist p = case (doesExist, p) of
  (False, P1 PNot (P0 (R0 (E0 (NestedQueryE (Left q)))))) ->
    Just <$> uncorrel q
  (True, P0 (R0 (E0 (NestedQueryE (Left q))))) ->
    Just <$> uncorrel q
  _ -> return Nothing

coerceESym :: (MonadError (UnnestError s e) m,HasCallStack) =>
             NestedQueryE s e -> m e
coerceESym = \case
  NestedQueryE (Left q)  -> throwAStr $ "Expected symbol got query: " ++ ashow q
  NestedQueryE (Right x) -> return x

-- | Use the uncorrelation axioms to rewrite the CorrelHead cases.
uncorrelFinal :: forall e s . (Eq e, Symbolic e,SymbolType e ~ String) =>
                Query e (CorrelHead e s)
              -> UnnestMonad e s (Query e s)
uncorrelFinal = mapTrav $ \case
    NoCorrel s -> return $ Q0 s
    ExistentialCorrel isExist ExistentialCorrelRecord{..} -> do
      a <- uncorrelFinal existCorrelSubTree
      b <- uncorrelFinal existCorrelQuery
      op <- joinOp
      return $ a `op` b
        where
          joinOp' = \case
            []   -> Q2 QProd
            p:ps -> J $ foldr And p ps
          joinOp = case (isExist, snd <$> pis) of
            (True, ps) -> return $ \a ->
              Q2 QDistinct a . joinOp' (toList p0M ++ ps) a
            -- There wasn't even one relation containing a free
            -- variable. The problem here is that left-triangle has no
            -- counterpart like join has product without introducing
            -- the notion of True.
            (False, [])   -> throwError $ NoNonfreeVarsInCorrel p0M
            (False, x:xs) -> return $ Q2 (QLeftAntijoin $ foldl And x xs)
          CorrelProp p0M pis = existCorrelSel
    GroupCorrel GroupCorrelRecord{..} -> do
      -- Here we have a selection and over that a total grouping.
      a <- uncorrelFinal grpCorrelSubTree
      b <- uncorrelFinal grpCorrelQuery
      op <- joinOp
      return $ a `op` b
        where
          -- | Selection prop of the nested query that do not contain
          -- free variables.
          selOp = case p0M of
            Just p0 -> S p0
            Nothing -> id
          joinOp = do
            usym <- unnestUniqueSymbol
            -- The Prop that has the nested query in it.
            outerProp <- grpCorrelProp usym
            -- The variables that need to be exposed because the will
            -- be used in the outer prop
            let boundVars :: [e] =
                  join [filter (isBoundVar freeVars) $ toList3 prop
                       | (freeVars, prop) <- pis]
            boundVarAssoc :: [(e,e)] <- forM boundVars
              $ \e -> (e,) <$> unnestUniqueSymbol
            let translateBoundVars :: Endo (Prop (Rel (Expr e))) =
                  -- The symbols are either literals or bound
                  fmap3 (\e -> fromMaybe e $ e `lookup` boundVarAssoc)
            let joinProp :: Prop (Rel (Expr e)) =
                  foldl And outerProp $ translateBoundVars . snd <$> pis
            let exposedBoundVars :: [(e,Expr (Aggr (Expr e)))] =
                  [(usym,E0 $ NAggr AggrFirst $ E0 bv)
                  | (bv,usym) <- boundVarAssoc]
            return $ \a -> Q2 QDistinct a
                          . J joinProp a
                          . groupOp ((usym,grpCorrelQueryVal):exposedBoundVars)
                          . selOp
          CorrelProp p0M pis = grpCorrelSel :: CorrelProp e
          groupOp grp = Q1 (QGroup grp $ getGroup =<< pis) where
            getGroup (freeVars,prop) =
              [E0 var | var <- toList3 prop, var `notElem` freeVars]

isBoundVar :: (Symbolic e, Eq e) => [e] -> e -> Bool
isBoundVar freeVars e = e `notElem` freeVars && isSymbol e

unnestUniqueSymbol :: (SymbolType e ~ String,Symbolic e) => UnnestMonad e s e
unnestUniqueSymbol = do
  r <- mkSymbol . ("unnested_query" ++) . show <$> get
  modify (+1)
  return r

mapTrav :: (Monad m, Traversable m, Applicative f) =>
          (a -> f (m b)) -> m a -> f (m b)
mapTrav fn = fmap join . traverse fn

uncorrelate :: (Eq e, Symbolic e,SymbolType e ~ String) =>
              Query (NestedQueryE s e) s
            -> UnnestMonad e s (Query e s)
uncorrelate = (return . normalize) >=> uncorrelInterm >=> uncorrelFinal

unnestQuery :: (Eq e,Symbolic e,SymbolType e ~ String) =>
              (e -> Query e s -> Maybe Bool)
            -> Query (NestedQueryE s e) s
            -> Either (UnnestError s e) (Query e s)
unnestQuery inQ = (`evalStateT` 0)
                  . (`runReaderT` inQ)
                  . uncorrelate
