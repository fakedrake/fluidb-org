{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
module Data.Query.QuerySchema.SchemaBase
  ( planSymOrig
  , isUniqueSel
  , mkQueryPlan
  , mkPlanSym
  , setPlanSymOrig
  , refersToPlan
  , querySchema
  , primKeys
  , joinPlans
  , planAllSyms
  , planProject
  , planSymIsSym
  , planSymTypeSym'
  , mkSymPlanSymNM
  , complementProj
  , mkLitPlanSym
  , schemaQP
  , lookupQP
  , planSymEqs
  , translatePlan
  , translatePlan'
  , translatePlanMap''
  ) where

import           Control.Monad.Reader
import           Data.Bitraversable
import           Data.CppAst
import           Data.Functor.Identity
import qualified Data.HashMap.Strict          as HM
import           Data.List
import qualified Data.List.NonEmpty           as NEL
import           Data.Maybe
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema.Types
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Hashable

mkLitPlanSym :: e -> PlanSym e s
mkLitPlanSym e = mkPlanSym (NonSymbolName e) e

mkSymPlanSymNM :: Hashables2 e s =>
                 NameMap e s -> e -> Either (QNFError e s) (PlanSym e s)
mkSymPlanSymNM nm e = (`mkPlanSym` e) . (`Column` 0) <$> getName nm e

planSymIsSym :: PlanSym e s -> Bool
planSymIsSym ps = case  planSymQnfName ps of
  NonSymbolName _ -> False
  _               -> True

planSymOrig :: PlanSym e s -> e
planSymOrig = planSymQnfOriginal

mkPlanSym :: QNFName e s -> e -> PlanSym e s
mkPlanSym qnfn e = PlanSym{planSymQnfOriginal=e,planSymQnfName=qnfn}

setPlanSymOrig :: e -> PlanSym e s -> PlanSym e s
setPlanSymOrig e ps = ps{planSymQnfOriginal=e}

-- | Nothing means it's a literal.
refersToPlan :: Hashables2 e s => PlanSym e s -> QueryPlan e s -> Maybe Bool
refersToPlan ps plan = case planSymQnfName ps of
  NonSymbolName _ -> Nothing
  _               -> Just $ isJust $ lookupQP ps plan

-- | NOT AN ISOMORPHISM
schemaQP :: QueryPlan e s -> [(PlanSym e s,ColumnProps)]
schemaQP = qpSchema

primKeys :: QueryPlan e s -> NEL.NonEmpty (NEL.NonEmpty (PlanSym e s))
primKeys = qpUnique

planAllSyms :: QueryPlan e s -> [PlanSym e s]
planAllSyms = fmap fst . schemaQP

querySchema :: QueryPlan e s -> CppSchema' (PlanSym e s)
querySchema
  plan = [(columnPropsCppType,ps) | (ps,ColumnProps {..}) <- schemaQP plan]

-- XXX: increment indices

-- | Filter the unique ones. None means there were no uniques (or the
-- plan was empty.
mkQueryPlan :: [(CppType,(e,Bool))] -> Maybe (QueryPlan' e)
mkQueryPlan sch =
  fromSchemaQP
    [(sym
     ,ColumnProps { columnPropsConst = False,columnPropsCppType = ty }
     ,uniq) | (ty,(sym,uniq)) <- sch]
  where
    fromSchemaQP :: [(e,ColumnProps,Bool)] -> Maybe (QueryPlan' e)
    fromSchemaQP
      sch = NEL.nonEmpty [s | (s,_,isU) <- sch,isU] <&> \uniq -> QueryPlan
      { qpSchema = [(s,prop) | (s,prop,_isU) <- sch],qpUnique = return uniq }


planSymTypeSym' :: Hashables2 e s =>
                  [QueryPlan e s]
                -> PlanSym e s
                -> Maybe (Either e CppType)
planSymTypeSym' plans ps = case planSymQnfName ps of
  NonSymbolName e -> Just $ Left e
  _ -> Right . columnPropsCppType
    <$> listToMaybe (catMaybes $ lookupQP ps <$> plans)
















































hardLookupQP :: Hashables2 e s =>
               PlanSym e s -> QueryPlan e s -> Maybe ColumnProps
hardLookupQP ps' = lookup ps' . schemaQP
lookupQP :: Hashables2 e s =>
           PlanSym e s -> QueryPlan e s -> Maybe ColumnProps
lookupQP = hardLookupQP
complementProj :: forall e s . Hashables2 e s =>
                 QueryPlan e s
               -> [(PlanSym e s, Expr (PlanSym e s))]
               -> [(PlanSym e s, Expr (PlanSym e s))]
complementProj plan prj = invPrj `union`
  [(e, E0 e) | e <- qsyms, e `notElem` map fst invPrj]
  where
    qsyms :: [PlanSym e s]
    qsyms = planAllSyms plan
    invPrj :: [(PlanSym e s, Expr (PlanSym e s))]
    invPrj = mapMaybe (invExpr . snd) prj

invExpr :: Expr (PlanSym e s)
        -> Maybe (PlanSym e s, Expr (PlanSym e s))
invExpr expr = runIdentity $ exprInverse (return . planSymIsSym) expr <&> \case
  EInv (x,toX) -> Just (x,toX x)
  _            -> Nothing


assocToMap :: Hashables2 e a => [(e,a)] -> HM.HashMap e [a]
assocToMap assoc = foldl' (\m (s,d) -> HM.alter (Just . maybe [d] (d:)) s m) mempty
  $ reverse assoc

-- | Given an association of in/out versions of the same symbol
-- translate a plan expressed in terms of input symbols as in terms of
-- output symbols. Note that plans should not have literals but we
-- pass them through.
--
-- Always hard lookups here.
translatePlan' :: forall e s . (HasCallStack, Hashables2 e s) =>
                 [(PlanSym e s, PlanSym e s)]
               -> QueryPlan e s
               -> Either (AShowStr e s) (QueryPlan e s)
translatePlan' = translatePlanMap'' listToMaybe

translatePlanMap'' :: forall e s . (HasCallStack, Hashables2 e s) =>
                     (forall a . [a] -> Maybe a)
                   -> [(PlanSym e s, PlanSym e s)]
                   -> QueryPlan e s
                   -> Either (AShowStr e s) (QueryPlan e s)
translatePlanMap'' f assoc p =
  traverse (bitraverse safeLookup pure) (qpSchema p)
  >>= \sch -> do
    qpu <- traverse2 safeLookup $ qpUnique p
    return QueryPlan{qpSchema=sch,qpUnique=qpu}
  where
    symsMap = assocToMap assoc
    safeLookup :: PlanSym e s -> Either (AShowStr e s) (PlanSym e s)
    safeLookup e = case planSymQnfName e of
      PrimaryCol{} -> undefined
      _            -> safeLookup0
      where
        safeLookup0 :: Either (AShowStr e s) (PlanSym e s)
        safeLookup0 = lookup_e `alt` asLiteral where
          asLiteral :: Either (AShowStr e s) (PlanSym e s)
          asLiteral = case planSymQnfName e of
            NonSymbolName _ -> Right e
            _               -> throwAStr $ "Expected literal:" ++ ashow e
          lookup_e :: Either (AShowStr e s) (PlanSym e s)
          lookup_e = case HM.lookup e symsMap >>= f of
            Nothing -> throwAStr $ "Lookup failed:"
              ++ ashow (e,planAllSyms p,HM.toList symsMap)
            Just x -> Right x
          -- | Prefer the left hand side error
          alt l@(Right _) _ = l
          alt _ r@(Right _) = r
          alt l _           = l

translatePlan :: (HasCallStack, Hashables2 e s) =>
                [(PlanSym e s, PlanSym e s)]
              -> QueryPlan e s
              -> Maybe (QueryPlan e s)
translatePlan = either (const Nothing) Just ... translatePlan'
isUniqueSel :: Hashables2 e s =>
              QueryPlan e s
            -> [(Expr (PlanSym e s),Expr (PlanSym e s))]
            -> Bool
isUniqueSel qp eqs = (`any` qpUnique qp) $ \ukeys ->
  (`all` ukeys)
  $ \uk -> (`any` (eqs ++ fmap flip eqs))
  $ \(el,er) ->
      E0 uk == el && not ((`any` er) $ \e -> e `elem` allKeys)
  where
    flip (a,b) = (b,a)
    allKeys = planAllSyms qp

planProject :: forall e s . Hashables2 e s =>
              ([QueryPlan e s] ->
               Expr (Either CppType (PlanSym e s)) ->
               Maybe ColumnProps)
            -> [(PlanSym e s, Expr (Either CppType (PlanSym e s)))]
            -> QueryPlan e s
            -> Maybe (QueryPlan e s)
planProject exprColumnProps prj plan = do
  symProps <- forM prj $ \(sym,expr) -> (sym,) <$> exprColumnProps [plan] expr
  uniq <- NEL.nonEmpty $ mapMaybe translUniq $ toList $ qpUnique plan
  return $ QueryPlan {qpSchema=symProps,
                      qpUnique=uniq}
  where
    keyAssoc = mapMaybe
      (\case {(e,E0 (Right ev)) -> Just (ev,e); _ -> Nothing})
      prj
    translUniq ukeys = mapM (`lookup` keyAssoc) ukeys

-- | Given an assoc and a list all the possible
-- λ> substitutions [('a','A')] ['a','b']
-- ('a' :| "b") :| ['A' :| "b"
-- λ> substitutions [('x','X')] ['a','b']
-- ('a' :| "b") :| []
substitutions :: forall a . Eq a =>
                [(a,a)] -> NEL.NonEmpty a -> NEL.NonEmpty (NEL.NonEmpty a)
substitutions eqs uniqs0 =
  foldl (\uniqs' eqpair -> disamb eqpair =<< uniqs') (return uniqs0) eqs
  where
    disamb (x,y) uniqs' =
      uniqs' NEL.:| toList (replace x y uniqs') ++ toList (replace y x uniqs')
    replace :: a -> a -> NEL.NonEmpty a -> Maybe (NEL.NonEmpty a)
    replace f t es = if f `elem` es
      then Just $ (\e -> if e == f then t else e) <$> es
      else Nothing

-- | Concatenate plans given an equality between them. The equality
-- makes for different unique sets.
joinPlans :: forall e . Eq e =>
            [(e,e)]
          -> QueryPlan' e
          -> QueryPlan' e
          -> Maybe (QueryPlan' e)
joinPlans eqs qpL qpR = uniqDropConst QueryPlan {
  qpSchema=sch,
  qpUnique=uniq
  }
  where
    sch = qpSchema qpL ++ qpSchema qpR
    uniq :: NEL.NonEmpty (NEL.NonEmpty e)
    uniq = do
      usetL :: NEL.NonEmpty e <- qpUnique qpL
      usetR :: NEL.NonEmpty e <- qpUnique qpR
      -- Check if the equalities are lookups
      let luOnL = all (`elem` fmap fst normEqs) usetL
      let luOnR = all (`elem` fmap snd normEqs) usetR
      case (luOnR,luOnL) of
        (False,True)  -> substitutions normEqs usetL
        (True,False)  -> substitutions (swap <$> normEqs) usetR
        (_,_)   -> NEL.nub
          $ substitutions normEqs usetL
          <> substitutions (swap <$> normEqs) usetR
    normEqs = mapMaybe (uncurry go) eqs
      where
        go l r = if | linl && not linr && rinr && not rinl -> Just (l,r)
                    | not linl && linr && not rinr && rinl -> Just (r,l)
                    | (linl && linr) || (rinl && rinr) ->
                      error "Exposed symbols should be disjoint in products"
                    | otherwise -> Nothing
          where
            linl = l `inAndNonConst` qpSchema qpL
            linr = l `inAndNonConst` qpSchema qpR
            rinl = r `inAndNonConst` qpSchema qpL
            rinr = r `inAndNonConst` qpSchema qpR
            inAndNonConst e s = case columnPropsConst <$> lookup e s of
              Just False -> True
              _          -> False

uniqDropConst :: Eq e => QueryPlan' e -> Maybe (QueryPlan' e)
uniqDropConst p = fmap (\uniq -> p{qpUnique=uniq})
  $ NEL.nonEmpty
  -- Note: if we have ONLY consts in a unique set something is wrong
  -- or we called this function at the wrong place (not over a
  -- product-like)
  $ mapMaybe (NEL.nonEmpty . filter isNotConst . toList)
  $ toList $ qpUnique p
  where
    isNotConst e = case fmap columnPropsConst $ lookup e $ qpSchema p of
      Just False -> True
      _          -> False

planSymEqs :: Hashables2 e s =>
             Prop (Rel (Expr (PlanSym e s)))
           -> [(PlanSym e s,PlanSym e s)]
planSymEqs p = mapMaybe asEq $ toList $ propQnfAnd p where
  asEq :: Prop (Rel (Expr (PlanSym e s))) -> Maybe (PlanSym e s,PlanSym e s)
  asEq = \case
    P0 (R2 REq (R0 (E0 l)) (R0 (E0 r))) ->
      if planSymIsSym l && planSymIsSym r
      then Just (l,r)
      else Nothing
    _ -> Nothing
