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
  , joinShapes
  , shapeProject
  , refersToShape
  , shapeSymOrig
  , setShapeSymOrig
  , mkSymShapeSymNM
  , mkShapeFromTbl
  , shapeSymIsSym
  , shapeSymTypeSym'
  , shapeAllSyms
  ) where

import           Control.Monad.Identity
import           Data.Codegen.Build.Monads.Class
import qualified Data.CppAst                          as CC
import           Data.Foldable
import           Data.Function
import           Data.List
import           Data.Maybe
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Query.QuerySchema.GetQueryShape
import           Data.Query.QuerySchema.Types
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Prelude                              hiding (exp)

type CppTypeExprConstraints c e s t n m = (
  Hashables2 e s,
  MonadCodeBuilder e s t n m, MonadCodeError e s t n m,
  Foldable c, Functor c,
  MonadReadScope c (QueryShape e s) m)

mkShapeFromTbl :: Hashables2 e s => QueryCppConf e s -> s -> Maybe (QueryShape e s)
mkShapeFromTbl QueryCppConf {..} s = do
  sch <- tableSchema s
  ucols <- uniqueColumns s
  mkQueryShape _rows
    $ fmap2 (\e -> (mkShapeSym (PrimaryCol e s 0) e,e `elem` ucols)) sch

-- |Get the schema from something that is like a projection.
anySchema :: forall c e s t n m a .
            (CppTypeExprConstraints c e s t n m) =>
            ((AShowV e, AShowV s) => a -> SExp)
          -> (a -> Expr (Either CC.CppType (ShapeSym e s)))
          -> [(ShapeSym e s, Expr a)]
          -> m (CppSchema' (ShapeSym e s))
anySchema ashowA toExpr proj = do
  QueryCppConf{..} <- getQueryCppConf
  shapes <- toList <$> getQueries
  forM proj $ \(prjSym, prjExprLike) ->
    maybe (err prjSym prjExprLike shapes) return
    $ (,prjSym) <$> exprCppType literalType shapes (toExpr =<< prjExprLike)
  where
    err prjSym prjExprLike shapes =
      throwAStr $ "anySchema " ++
      ashow (ashowA <$> prjExprLike, fmap (fmap f . shapeAllSyms) shapes)
      where
        f x = (prjSym, x)

-- |We assume that aggregation functions are group operators (that is
-- a x a -> a)
aggrSchema :: forall c e s t n m .
             CppTypeExprConstraints c e s t n m =>
             [(ShapeSym e s, Expr (Aggr (Expr (ShapeSym e s))))]
           -> m (CppSchema' (ShapeSym e s))
aggrSchema = anySchema ashow' $ fmap Right . unAggr

projSchema
  :: forall c e s t n m .
  CppTypeExprConstraints c e s t n m
  => [(ShapeSym e s,Expr (Either CC.CppType (ShapeSym e s)))]
  -> m (CppSchema' (ShapeSym e s))
projSchema = anySchema ashow' return

keysToProj :: ExprLike e a => [e] -> [(e, a)]
keysToProj keys = [(k, asExpr $ E0 k) | k <- keys]
withPrimKeys :: forall m e s t n a .
               (HasCallStack, CppTypeExprConstraints Identity e s t n m,
                ExprLike (ShapeSym e s) a,
                Eq a) =>
               [(ShapeSym e s, a)] -> m [(ShapeSym e s, a)]
withPrimKeys prj = do
  Identity q <- getQueries @Identity @e @s
  unless (any (all (`elem` fmap fst prj)) $ primKeys q)
    $ throwAStr "No subset of columns are unique."
  return prj
exprCppTypeErr :: forall c e s t n m .
                 (HasCallStack, CppTypeExprConstraints c e s t n m) =>
                 Expr (ShapeSym e s)
               -> m CC.CppType
exprCppTypeErr exp = do
  QueryCppConf {..} <- cbQueryCppConf <$> getCBState
  shapes <- toList <$> getQueries
  let shapeType shapeSym = case shapeSymType literalType shapes shapeSym of
        Nothing -> throwAStr
          $ "exprCppTypeErr: Can't find type for: "
          ++ ashow
            (shapeSym
            ,querySchema <$> shapes
            ,lookup shapeSym . fmap swap . querySchema <$> shapes
            ,exp)
        Just ty -> return (shapeSymOrig shapeSym,ty)
  expT <- traverse shapeType exp
  case exprCppType' $ snd <$> expT of
    Just ty -> return ty
    Nothing -> throwAStr $ "exprCppTypeErr " ++ ashow expT

cppSchema
  :: forall e s t n m .
  (Hashables2 e s,MonadCodeError e s t n m,CC.ExpressionLike e)
  => CppSchema' (ShapeSym e s)
  -> m CppSchema
cppSchema = traverse extractSym where
  extractSym :: (CC.CppType, ShapeSym e s) -> m (CC.CppType, CC.CodeSymbol)
  extractSym (ty, e) = (ty,) <$> case CC.toExpression e of
    CC.SymbolExpression (CC.Symbol esym) -> return esym
    _                                    -> throwCodeErr $ ExpectedSymbol e
