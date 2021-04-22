{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.EquiJoinUtils
  ( EquiJoinPred(..)
  , equiJoinPred
  , mkEquiJoinPred
  ) where

import Data.Utils.ListT
import Data.Utils.Tup
import           Control.Monad.Identity
import           Control.Monad.Trans
import           Data.List
import           Data.Maybe
import           Data.Monoid
import           Data.String
import           Data.Query.Algebra
import           Data.Codegen.Build.Classes
import           Data.Codegen.Build.Expression
import           Data.Codegen.Build.Monads
import           Data.Codegen.Schema
import qualified Data.CppAst                   as CC
import           Data.Query.QuerySchema
import           Prelude                                  hiding (exp)

-- An equivalence class between two indexed queries
data QueryEquivClass e = QueryEquivClass {
  -- The expression for the left query
  equivClsLExp :: Expr e,
  -- Which query from the scope is the left query
  -- equivClsLIndex :: Int,
  -- The expression for the right query
  equivClsRExp :: Expr e
  -- Which query from the scope is the right query
  -- equivClsRIndex :: Int
  }
data EquiJoinPred = EquiJoinPred {
  equiJoinLExtract :: CC.Class CC.CodeSymbol,
  equiJoinRExtract :: CC.Class CC.CodeSymbol,
  equiJoinKey      :: CC.Class CC.CodeSymbol
  }

-- | Gather simulateneous equiv classes to a a join predicate.
mkEquiJoinPred :: forall e s t n m .
                 (MonadCodeCheckpoint e s t n m,
                  MonadSchemaScope Tup2 e s m) =>
                 [QueryEquivClass (PlanSym e s)]
               -> m EquiJoinPred
mkEquiJoinPred qeCls = do
  sch <- forM (zip [0::Int ..] qeCls) $ \(i, QueryEquivClass{..}) -> do
    Tup2 tyl tyr <- exprCppTypeErr `traverse` Tup2 equivClsLExp equivClsRExp
    let sym = fromString $ "sortElem" ++ show i
    if tyl == tyl
      then return (tyr, sym)
      else throwCodeErrStr "Mismatch between types equivClsLExp equivClsRExp"
  Tup2 ql qr <- getQueries @Tup2 @e @s
  equiJoinKey <- tellRecordCls sch
  let equiJoinKeySym = CC.classNameRef equiJoinKey
  equiJoinLExtract <- extractKeyFrom equivClsLExp equiJoinKeySym ql
  equiJoinRExtract <- extractKeyFrom equivClsRExp equiJoinKeySym qr
  return EquiJoinPred{..}
  where
    mkExtractFnClass equiJoinKeySym prjM = tellClassM $ do
      prj <- prjM
      mkCallClass (CC.ClassType mempty [] equiJoinKeySym)
        $ CC.FunctionAp (CC.SimpleFunctionSymbol equiJoinKeySym) [] prj
    extractKeyFrom equivClsLRExp equiJoinKeySym q = evalQueryEnv (Identity q)
      $ mkExtractFnClass equiJoinKeySym
      $ forM qeCls
      $ exprExpression . fmap Right . equivClsLRExp

equiJoinPred :: forall e s t n m .
               (MonadSchemaScope Tup2 e s m,
                MonadCodeBuilder e s t n m, MonadCodeError e s t n m) =>
               Prop (Rel (Expr (PlanSym e s)))
             -> m (Maybe [QueryEquivClass (PlanSym e s)])
equiJoinPred p = maybe (return Nothing) (fmap sequenceA . mapM go) $ eqPairs p
  where
    go :: (Expr (PlanSym e s), Expr (PlanSym e s))
       -> m (Maybe (QueryEquivClass (PlanSym e s)))
    go (e1,e2) = headListT $ do
      Tup2 pl pr <- lift $ getQueries @Tup2 @e @s
      (el,er) <- mkListT $ return [(e1,e2),(e2,e1)]
      let refsTo e p = all (fromMaybe True . (`refersToPlan` p)) e
      guard $ el `refsTo` pl && er `refsTo` pr
        -- Make sure we aren't actually dealing with literals or sth.
        && not (el `refsTo` pr || er `refsTo` pl)
      return QueryEquivClass{equivClsLExp=el, equivClsRExp=er}
    eqPairs :: Prop (Rel b) -> Maybe [(b, b)]
    eqPairs (And (P0 (R2 REq (R0 a) (R0 b))) x) = ((a,b):) <$> eqPairs x
    eqPairs (And x (P0 (R2 REq (R0 a) (R0 b)))) = ((a,b):) <$> eqPairs x
    eqPairs (And x y)                           = (++) <$> eqPairs x <*> eqPairs y
    eqPairs (P0 (R2 REq (R0 a) (R0 b)))         = Just [(a, b)]
    eqPairs _                                   = Nothing
