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
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.Expression
  ( propExpression
  , translateSym
  , getQueryDeclarations
  , exprExpression
  , projExpression
  , aggrProjExpression
  , tellRecordCls
  , anyProjExpression
  , lookupQP
  ) where

import Data.Utils.Functors
import Data.Utils.Function
import Data.Query.QuerySchema.Types
import Data.Utils.Hashable
import           Control.Monad.Identity
import           Data.Either
import           Data.Foldable
import           Data.List
import           Data.Maybe
import           Data.Monoid
import qualified Data.Set                             as DS
import           Data.String
import           Data.Tuple
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Codegen.Build.Monads
import           Data.Codegen.Schema
import qualified Data.CppAst               as CC
import           Data.Query.QuerySchema
import           Prelude                              hiding (exp)

getQueryDeclarations :: forall c e s t n m .
                       (Hashables2 e s,
                        MonadCodeBuilder e s t n m,
                        MonadCodeError e s t n m,
                        MonadCheckpoint m, MonadSoftCodeBuilder m,
                        Traversable c,
                        CC.ExpressionLike e,
                        MonadReadScope c (QueryShape e s) m) =>
                       m (c (QueryShape e s, CC.Declaration CC.CodeSymbol))
getQueryDeclarations =
  runScopeEnv <$> getScopeQueries >>= traverse (\x -> (fst x,) <$> mkArg x)
  where
    mkArg :: (QueryShape e s, CC.Symbol CC.CodeSymbol)
          -> m (CC.Declaration CC.CodeSymbol)
    mkArg (plan, usym) = do
      schema <- cppSchema $ querySchema plan
      record <- CC.classNameRef <$> tellRecordCls schema
      return CC.Declaration {
        declarationType=CC.ClassType
          (DS.fromList [CC.CppConst, CC.Reference]) [] record,
          declarationName=usym
        }

translateSym :: (HasCallStack, Hashables2 e s, MonadCodeError e s t n m) =>
               [(ShapeSym e s,ShapeSym e s)]
             -> ShapeSym e s -> m (ShapeSym e s)
translateSym assoc sym = if shapeSymIsSym sym
  then maybe (throwAStr $ "Lookup err:" ++ ashow (sym,assoc)) return
       $ lookup sym assoc
  else return sym

-- |Make a singleton expression corresponding to the symbol.
--
symbolExpression :: forall c e s t n m .
                   (Hashables2 e s, Monad m, CC.ExpressionLike e, HasCallStack,
                    MonadReadScope c (QueryShape e s) m,
                    MonadCodeBuilder e s t n m,
                    Traversable c, Functor c, Foldable c,
                    MonadCheckpoint m, MonadSoftCodeBuilder m,
                    MonadCodeError e s t n m) =>
                   ShapeSym e s
                 -> m (CC.Expression CC.CodeSymbol)
symbolExpression eSym = do
  declNames <- fmap3 CC.declarationNameRef getQueryDeclarations
  case lookupPlanRef eSym $ toList declNames of
    Nothing -> case CC.toExpression eSym of
      CC.SymbolExpression _ -> do
        ps <- toList <$> getQueries @c @e @s
        throwAStr $ "Couldn't match symbol with a plans: " ++ ashow (eSym,ps)
      x -> return x
    Just recSym -> case CC.toExpression eSym of
      CC.SymbolExpression s -> return
        $ CC.ObjectMember (CC.SymbolExpression recSym) s
      _ -> throwAStr $ "Invalid symbol: " ++ ashow eSym

lookupPlanRef :: Hashables2 e s =>
                ShapeSym e s
              -> [(QueryShape e s, b)]
              -> Maybe b
lookupPlanRef e = fmap snd
  . listToMaybe
  . filter (isJust . lookupQP e . fst)

softFunction :: CC.Symbol a
             -> CC.Expression a
             -> CC.Expression a
softFunction s e = CC.FunctionAp (CC.SimpleFunctionSymbol s) [] [e]

-- | Build and register class for schema. If a duplicate is create
-- drop all side effects and use the original one.
tellRecordCls
  :: (MonadCodeBuilder e s t n m
     ,MonadCheckpoint m
     ,MonadSoftCodeBuilder m
     ,MonadCodeError e s t n m)
  => CppSchema
  -> m (CC.Class CC.CodeSymbol)
tellRecordCls sch = tellClassM $ do
  name <- mkUSymbol "Record"
  return $ CC.recordCls name sch

anyProjExpression
  :: forall e s t n m a f .
  (Hashables2 e s
  ,CC.ExpressionLike e
  ,Applicative f
  ,MonadCodeBuilder e s t n m
  ,MonadCheckpoint m
  ,MonadCodeError e s t n m
  ,MonadSoftCodeBuilder m)
  => ([(ShapeSym e s,Expr a)] -> m (CppSchema' (ShapeSym e s)))
  -> (Expr a -> m (CC.Expression (f CC.CodeSymbol)))
  -> [(ShapeSym e s,Expr a)]
  -> m (CC.Expression (f CC.CodeSymbol))
anyProjExpression toSchema toExpression proj = do
  schema <- cppSchema =<< toSchema proj
  recordDef <- tellRecordCls schema
  let recordRef = pure <$> CC.classNameRef recordDef
  CC.FunctionAp (CC.SimpleFunctionSymbol recordRef) []
    <$> (mapM (toExpression . snd) proj :: m [CC.Expression (f CC.CodeSymbol)])

aggrProjExpression :: forall c e s t n m .
                     (Hashables2 e s, CC.ExpressionLike e,
                      MonadCodeCheckpoint e s t n m,
                      Foldable c, Functor c, Traversable c,
                      MonadReadScope c (QueryShape e s) m) =>
                     [(ShapeSym e s, Expr (Aggr (Expr (ShapeSym e s))))]
                   -> m (CC.Expression
                        (Either (CC.Declaration CC.CodeSymbol) CC.CodeSymbol))
aggrProjExpression = anyProjExpression aggrSchema aggrExpression

projExpression :: forall c e s t n m .
                 (Hashables2 e s, CC.ExpressionLike e, HasCallStack,
                  MonadCodeBuilder e s t n m, MonadCodeError e s t n m,
                  MonadCheckpoint m, Foldable c, Functor c, Traversable c,
                  MonadReadScope c (QueryShape e s) m, MonadSoftCodeBuilder m) =>
                 [(ShapeSym e s, Expr (Either CC.CppType (ShapeSym e s)))]
               -> m (CC.Expression CC.CodeSymbol)
projExpression = fmap3 runIdentity
  $ anyProjExpression projSchema
  $ fmap2 Identity . exprExpression

-- | Write C++ with explicit prescedence params.
anyExprExpression :: forall e' e s t n m f .
                    (Monad m, Applicative f) =>
                    (e' -> m (CC.Expression (f CC.CodeSymbol)))
                  -> Expr e'
                  -> m (CC.Expression (f CC.CodeSymbol))
anyExprExpression symExp = go 0 where
  go :: Int -> Expr e' -> m (CC.Expression (f CC.CodeSymbol))
  go pre = \case
    -- Low prescedence means do this first. If we are lowering prescedence
    E2 o l r -> (if opPrescedence o > pre then fmap CC.Parens else id)
      $ case o of
        ELike -> CC.FunctionAp
                (CC.SimpleFunctionSymbol $ CC.Symbol $ pure "like")
                []
                <$> mapM (go 0) [l, r]
        -- clang throws warnings when mixing ands and ors.
        EAnd -> fmap CC.Parens expression
        EOr -> fmap CC.Parens expression
        _ -> expression
        where
          expression = CC.E2ession (CC.Operator $ CC.toCode o)
                       <$> go (opPrescedence o) l
                       <*> go (opPrescedence o) r
    E1 o e -> case o of
      EFun fun ->
        function (CC.Symbol $ pure $ CC.CppLiteralSymbol $ CC.toCode fun) e
      ENot     -> function (CC.Symbol $ pure "!") e
      ENeg     -> CC.Parens
        <$> function (CC.Symbol $ pure "-") e
      EAbs -> function (CC.Symbol $ pure "abs") e
      ESig -> function (CC.Symbol $ pure "sig") e
    E0 s -> symExp s
    where
      function s e = softFunction s <$> go 0 e

exprExpression :: forall c e s t n m .
                 (Hashables2 e s, CC.ExpressionLike e, MonadCheckpoint m,
                  MonadSoftCodeBuilder m, HasCallStack,
                  Traversable c,
                  MonadReadScope c (QueryShape e s) m, MonadCodeBuilder e s t n m,
                  MonadCodeError e s t n m) =>
                 Expr (Either CC.CppType (ShapeSym e s))
               -> m (CC.Expression CC.CodeSymbol)
exprExpression =
  fmap3 runIdentity
  $ anyExprExpression
  $ fmap2 Identity
  . either natNull symbolExpression
  where
    natNull :: CC.CppType -> m (CC.Expression CC.CodeSymbol)
    natNull = (return .) $ \case
      CC.CppInt -> CC.LiteralIntExpression 0
      CC.CppArray CC.CppChar size ->
        CC.FunctionAp "fluidb_string" [CC.TemplArg $ CC.Quote $ CC.toCode size] []
      CC.CppArray _ _ -> undefined
      CC.CppDouble -> CC.LiteralFloatExpression 0
      CC.CppChar -> CC.Quote "'\\0'"
      CC.CppNat -> CC.LiteralIntExpression $ -1
      CC.CppVoid -> CC.LiteralNullExpression
      CC.CppBool -> CC.LiteralBoolExpression False

-- updating the counter anyway.
aggrExpression :: forall c' c e s t n m .
                 (Hashables2 e s, MonadSoftCodeBuilder m,
                  MonadReadScope c (QueryShape e s) m, MonadCodeBuilder e s t n m,
                  Traversable c,
                  MonadCheckpoint m,
                  MonadCodeError e s t n m,
                  CC.ExpressionLike e) =>
                 Expr (Aggr (Expr (ShapeSym e s)))
               -> m (CC.Expression
                    (Either (CC.Declaration CC.CodeSymbol) CC.CodeSymbol))
aggrExpression = anyExprExpression buildFunc where
  buildFunc :: Aggr (Expr (ShapeSym e s))
            -> m (CC.Expression
                 (Either
                  (CC.Declaration CC.CodeSymbol)
                  CC.CodeSymbol))
  buildFunc (NAggr af e) = softFunction
                           <$> mkStatefulFunc e af
                           <*> fmap2 Right (exprExpression $ Right <$> e)
    where
      mkStatefulFunc :: Expr (ShapeSym e s)
                     -> AggrFunction
                     -> m (CC.Symbol
                          (Either (CC.Declaration CC.CodeSymbol) CC.CodeSymbol))
      mkStatefulFunc exp f = fmap Left . CC.Symbol <$> do
        expType <- exprCppTypeErr exp
        let ty = CC.TypeArg $ CC.PrimitiveType mempty expType
        sym <- mkUSymbol ("v" ++ show f)
        return CC.Declaration {
          declarationName=sym,
          declarationType=CC.ClassType mempty [ty] $ fromString $ show f}

-- | The plan symbols refer to the output plan which is provided but
-- in the c++ code they should refer to the input nodes.
propExpression :: forall c e s t n m .
                 (Hashables2 e s, CC.ExpressionLike e, HasCallStack,
                  MonadCodeBuilder e s t n m, MonadCodeError e s t n m,
                  Traversable c,
                  MonadCheckpoint m, MonadSoftCodeBuilder m,
                  MonadReadScope c (QueryShape e s) m) =>
                 Prop (Rel (Expr (Either CC.CppType (ShapeSym e s))))
               -> m (CC.Expression CC.CodeSymbol)
propExpression = recur where
  recur = \case
    -- Low prescedence means do this first. If we are lowering prescedence
    P2 o l r -> (CC.Parens
                 ...
                 CC.E2ession (CC.Operator $ CC.toCode o))
               <$> recur l <*> recur r
    P1 o p -> CC.Parens
      . softFunction (CC.Symbol $ CC.CppLiteralSymbol $ CC.toCode o)
      <$> recur p
    P0 r -> relExpression @c @e @s r

relExpression :: forall c e s t n m .
                (Hashables2 e s, HasCallStack,
                 MonadCodeBuilder e s t n m, MonadCodeError e s t n m,
                 Traversable c,
                 MonadCheckpoint m, MonadSoftCodeBuilder m,
                 MonadReadScope c (QueryShape e s) m, CC.ExpressionLike e) =>
                Rel (Expr (Either CC.CppType (ShapeSym e s)))
              -> m (CC.Expression CC.CodeSymbol)
relExpression = recur where
  recur = \case
    -- Low prescedence means do this first. If we are lowering prescedence
    R2 o l r -> case o of
      RLike -> func
      RSubstring -> func
      _ -> (CC.Parens
           ... CC.E2ession (CC.Operator $ CC.toCode o))
          <$> recur l
          <*> recur r
      where
        func = CC.FunctionAp
          (CC.SimpleFunctionSymbol $ CC.Symbol $ CC.CppSymbol $ CC.toCode o) []
          <$> mapM recur [l, r]
    R0 e -> exprExpression @c @e @s e
