{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing #-}

module Data.Codegen.Schema
  ( cppSchema
  , exprCppTypeErr
  , exprCppType
  , querySchema
  , aggrSchema
  , projSchema
  , withPrimKeys
  , keysToProj
  , primKeys
  , joinPlans
  , planProject
  , refersToPlan
  , planSymOrig
  , setPlanSymOrig
  , mkSymPlanSymNM
  , mkPlanFromTbl
  , planSymIsSym
  , planSymTypeSym'
  , planAllSyms
  ) where

import Data.Utils.Functors
import Data.Query.QuerySchema.Types
import Data.Utils.Hashable
import           Control.Monad.Identity
import           Data.Foldable
import           Data.Function
import           Data.List
import           Data.Maybe
import           Data.Tuple
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.QnfQuery.Types
import           Data.Codegen.Build.Monads.Class
import qualified Data.CppAst                     as CC
import           Data.Query.QuerySchema
import           Data.Query.QuerySchema.GetQueryPlan
import           Prelude                                    hiding (exp)

type CppTypeExprConstraints c e s t n m = (
  Hashables2 e s,
  MonadCodeBuilder e s t n m, MonadCodeError e s t n m,
  Foldable c, Functor c,
  MonadReadScope c (QueryPlan e s) m)

mkPlanFromTbl :: Hashables2 e s => QueryCppConf e s -> s -> Maybe (QueryPlan e s)
mkPlanFromTbl QueryCppConf{..} s = do
  sch <- tableSchema s
  ucols <- uniqueColumns s
  mkQueryPlan
    $ fmap2 (\e -> (mkPlanSym (PrimaryCol e s 0) e,e `elem` ucols)) sch

-- |Get the schema from something that is like a projection.
anySchema :: forall c e s t n m a .
            (CppTypeExprConstraints c e s t n m) =>
            ((AShowV e, AShowV s) => a -> SExp)
          -> (a -> Expr (Either CC.CppType (PlanSym e s)))
          -> [(PlanSym e s, Expr a)]
          -> m (CppSchema' (PlanSym e s))
anySchema ashowA toExpr proj = do
  QueryCppConf{..} <- getQueryCppConf
  plans <- toList <$> getQueries
  forM proj $ \(prjSym, prjExprLike) ->
    maybe (err prjSym prjExprLike plans) return
    $ (,prjSym) <$> exprCppType literalType plans (toExpr =<< prjExprLike)
  where
    err prjSym prjExprLike plans =
      throwAStr $ "anySchema " ++
      ashow (ashowA <$> prjExprLike, fmap (fmap f . planAllSyms) plans)
      where
        f x = (prjSym, x)

-- |We assume that aggregation functions are group operators (that is
-- a x a -> a)
aggrSchema :: forall c e s t n m .
             CppTypeExprConstraints c e s t n m =>
             [(PlanSym e s, Expr (Aggr (Expr (PlanSym e s))))]
           -> m (CppSchema' (PlanSym e s))
aggrSchema = anySchema ashow' $ fmap Right . unAggr

projSchema :: forall c e s t n m .
             CppTypeExprConstraints c e s t n m =>
             [(PlanSym e s, Expr (Either CC.CppType (PlanSym e s)))]
           -> m (CppSchema' (PlanSym e s))
projSchema = anySchema ashow' return

keysToProj :: ExprLike e a => [e] -> [(e, a)]
keysToProj keys = [(k, asExpr $ E0 k) | k <- keys]
withPrimKeys :: forall m e s t n a .
               (HasCallStack, CppTypeExprConstraints Identity e s t n m,
                ExprLike (PlanSym e s) a,
                Eq a) =>
               [(PlanSym e s, a)] -> m [(PlanSym e s, a)]
withPrimKeys prj = do
  Identity q <- getQueries @Identity @e @s
  unless (any (all (`elem` fmap fst prj)) $ primKeys q)
    $ throwAStr "No subset of columns are unique."
  return prj
exprCppTypeErr :: forall c e s t n m .
                 (HasCallStack, CppTypeExprConstraints c e s t n m) =>
                 Expr (PlanSym e s)
               -> m CC.CppType
exprCppTypeErr exp = do
  QueryCppConf{..} <- cbQueryCppConf <$> getCBState
  plans <- toList <$> getQueries
  let planType planSym = case planSymType literalType plans planSym of
        Nothing -> throwAStr $ "exprCppTypeErr: Can't find type for: "
          ++ ashow (planSym,
                    querySchema <$> plans,
                    lookup planSym . fmap swap . querySchema <$> plans,
                    exp)
        Just ty  -> return (planSymOrig planSym, ty)
  expT <- traverse planType exp
  case exprCppType' $ snd <$> expT of
   Just ty -> return ty
   Nothing -> throwAStr $ "exprCppTypeErr " ++ ashow expT

cppSchema
  :: forall e s t n m .
  (Hashables2 e s,MonadCodeError e s t n m,CC.ExpressionLike e)
  => CppSchema' (PlanSym e s)
  -> m CppSchema
cppSchema = traverse extractSym where
  extractSym :: (CC.CppType, PlanSym e s) -> m (CC.CppType, CC.CodeSymbol)
  extractSym (ty, e) = (ty,) <$> case CC.toExpression e of
    CC.SymbolExpression (CC.Symbol esym) -> return esym
    _                                    -> throwCodeErr $ ExpectedSymbol e
