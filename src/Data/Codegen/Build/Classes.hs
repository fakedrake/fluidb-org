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
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.Classes
  (bundleTransitions
  ,mkCallClass
  ,combinerClass
  ,propCallClass
  ,startNewCallClass
  ,projToFn
  ,schemaProjectionClass
  ,ashowCout
  ,getQueryDeclarations
  ,queryRecord
  ,flipOutShape
  ,UnDir(..)
  ,BinDir(..)
  ,OutShape(..)
  ,TransitionBundle(..)) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Data.Cluster.Types
import           Data.Codegen.Build.Expression
import           Data.Codegen.Build.IoFiles
import           Data.Codegen.Build.Monads
import           Data.Codegen.Schema
import           Data.CppAst
import qualified Data.CppAst                   as CC
import           Data.Foldable
import           Data.List
import           Data.Monoid
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.QuerySchema.Types
import           Data.QueryPlan.Types
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL
import           Data.Utils.Tup
import           Prelude                       hiding (exp)
import           Text.Printf

data TransitionBundle e s t n =
  ForwardTransitionBundle (Tup2 [NodeRef n]) (AnyCluster e s t n)
  | ReverseTransitionBundle (Tup2 [NodeRef n]) (AnyCluster e s t n)
  | DeleteTransitionBundle (NodeRef n)
  deriving Eq

mkTransitionBundle
  :: (Hashables2 e s
     ,MonadReader (ClusterConfig e s t n) m
     ,MonadAShowErr e s err m)
  => Transition t n
  -> m (TransitionBundle e s t n)
mkTransitionBundle = dropState (ask,const $ return ()) . \case
  RTrigger i t o -> do
    clust <- snd <$> getClusterT t
    -- Reversed IO for reverse trigger
    let i' = filter (`elem` clusterOutputs clust) i
    let o' = filter (`elem` clusterInputs clust) o
    return $ ReverseTransitionBundle (Tup2 i' o') clust
  Trigger i t o -> do
    clust <- snd <$> getClusterT t
    let i' = filter (`elem` clusterInputs clust) i
    let o' = filter (`elem` clusterOutputs clust) o
    return $ ForwardTransitionBundle (Tup2 i' o') clust
  DelNode n -> return $ DeleteTransitionBundle n

bundleTransitions
  :: forall e s t n m err .
  (Hashables2 e s
  ,MonadReader (ClusterConfig e s t n) m
  ,MonadAShowErr e s err m)
  => [Transition t n]
  -> m [TransitionBundle e s t n]
bundleTransitions
  trns = foldl mergeTransBundles [] <$> traverse mkTransitionBundle trns
  where
    mergeTransBundles
      :: [TransitionBundle e s t n]
      -> TransitionBundle e s t n
      -> [TransitionBundle e s t n]
    mergeTransBundles [] x = [x]
    mergeTransBundles (b:bs) x = case (b,x) of
      (ForwardTransitionBundle (Tup2 i o) c
        ,ForwardTransitionBundle (Tup2 i' o') c') -> if c == c'
        then ForwardTransitionBundle (Tup2 (nub $ i ++ i') (nub $ o ++ o')) c'
          : bs else x : b : bs
      (ReverseTransitionBundle (Tup2 i o) c
        ,ReverseTransitionBundle (Tup2 i' o') c') -> if c == c'
        then ReverseTransitionBundle (Tup2 (nub $ i ++ i') (nub $ o ++ o')) c'
          : bs else x : b : bs
      _ -> x : b : bs

-- Call classes
type MonadClassBuilder c e s t n m =
  (MonadCodeError e s t n m
  ,MonadCodeBuilder e s t n m
  ,MonadCheckpoint m
  ,CC.ExpressionLike e
  ,MonadSoftCodeBuilder m
  ,MonadSchemaScope c e s m)
-- | Make a call class with a single static class
mkCallClass :: forall c e s t n m .
              MonadClassBuilder c e s t n m =>
              CC.Type CC.CodeSymbol
            -> CC.Expression CC.CodeSymbol
            -> m (CC.Class CC.CodeSymbol)
mkCallClass codomainType ex = mkCallClass' codomainType [CC.ReturnSt ex]

mkCallClass' :: forall c e s t n m .
               MonadClassBuilder c e s t n m =>
               CC.Type CC.CodeSymbol
             -> [CC.Statement CC.CodeSymbol]
             -> m (CC.Class CC.CodeSymbol)
mkCallClass' codomainType body = do
  decls <- fmap snd . toList <$> getQueryDeclarations
  clsName <- mkUSymbol "CallableClass"
  let func = CC.Function {
        functionName="operator()",
        functionType=codomainType,
        functionBody=body,
        functionArguments=CC.Argument <$> decls,
        functionConstMember=False
        }
  return CC.Class {
    className=clsName,
    classConstructors=[],
    classPublicFunctions=[func],
    classPublicMembers=[],
    classTypeDefs=CC.TypeDef
      (CC.clearTypeModifiers codomainType) (CC.Symbol "Codomain"): do
        (i, CC.Declaration{..}) <- zip [0 :: Int ..] decls
        return $ CC.TypeDef (CC.clearTypeModifiers declarationType)
          (CC.Symbol $ fromString $ "Domain" ++ show i),
    classPrivateMembers=[]}

data UnDir f where
  UnFwd :: f ~ Identity => UnDir f
  UnRev :: f ~ Tup2 => UnDir f
data BinDir f where
  BinFwd :: f ~ Tup2 => BinDir f
  BinRev :: f ~ Identity => BinDir f


data OutShape e s =
  OutShape
  { oaIoAssoc :: [(ShapeSym e s,ShapeSym e s)]
   ,oaShape   :: QueryShape e s
  }

flipOutShape :: OutShape e s -> OutShape e s
flipOutShape oa = oa { oaIoAssoc = swap <$> oaIoAssoc oa }

combinerClass
  :: forall e s t n m .
  (MonadSchemaScope Tup2 e s m,MonadCodeCheckpoint e s t n m)
  => OutShape e s
  -> m (CC.Class CC.CodeSymbol)
combinerClass OutShape{..} = do
  Tup2 (q1,qs1) (q2,qs2) :: Tup2 (QueryShape e s,Symbol CodeSymbol)
    <- fmap3 CC.declarationNameRef getQueryDeclarations
  let symAssoc1 = mkPs qs1 . snd <$> querySchema q1
  let symAssoc2 = mkPs qs2 . snd <$> querySchema q2
  outCppSchema :: CppSchema <- cppSchema $ querySchema oaShape
  resType <- CC.classNameRef <$> tellRecordCls outCppSchema
  let outSyms = snd <$> querySchema oaShape
  case orderAssoc (\o i -> (i,o) `elem` oaIoAssoc) outSyms $ symAssoc1 ++ symAssoc2 of
    Right symAssocComb -> tellClassM
      $ mkCallClass (CC.ClassType mempty [] resType)
      $ CC.FunctionAp (CC.SimpleFunctionSymbol resType) [] symAssocComb
    Left x -> throwAStr
      $ printf
        "the input plans :\n%s\n don't match the output plan:\n%s\nConflicting sym:%s"
        (ashow $ fmap fst $ symAssoc1 ++ symAssoc2)
        (ashow outSyms)
        (ashow x)
  where
    mkPs :: Symbol CodeSymbol
         -> ShapeSym e s
         -> (ShapeSym e s,CC.Expression CC.CodeSymbol)
    mkPs recSym shapeSym = case CC.toExpression shapeSym of
      CC.SymbolExpression cppSym
        -> (shapeSym,CC.ObjectMember (CC.SymbolExpression recSym) cppSym)
      _ -> error "Unreachable.."
lookupWith :: (a -> a -> Bool) ->  a -> [(a,b)] -> Maybe b
lookupWith f a assoc = case filter (f a . fst) assoc of
  []      -> Nothing
  (_,x):_ -> Just x
orderAssoc :: (b -> b -> Bool) -> [b] -> [(b,a)] -> Either b [a]
orderAssoc cmp order assoc =
  (\x -> maybe (Left x) Right $ lookupWith cmp x assoc)
  `traverse` order

propCallClass
  :: (MonadCodeCheckpoint e s t n m,MonadSchemaScope c e s m)
  => Prop (Rel (Expr (ShapeSym e s)))
  -> m (CC.Class CC.CodeSymbol)
propCallClass = tellClassM
  . mkCallClass (CC.PrimitiveType mempty CC.CppBool)
  <=< (propExpression . fmap3 Right)


-- |Make a static type to remember each expression and a bool type
-- indicating if it's instantiated. The call function should return
-- true when any of the exprs evaluates to something different than
-- the cached.
startNewCallClass :: (MonadCodeCheckpoint e s t n m,
                     MonadSchemaScope c e s m) =>
                    [Expr (ShapeSym e s)]
                  -> m (CC.Class CC.CodeSymbol)
startNewCallClass exps = tellClassM $ do
  -- Extract info about the expreesions
  expsArg <- forM exps $ \exp -> do
    sym <- mkUSymbol "staticExp"
    codeExp <- exprExpression $ Right <$>  exp
    expType <- exprCppTypeErr exp
    let et = CC.PrimitiveType mempty expType
    return (CC.Declaration {CC.declarationName=sym, CC.declarationType=et},
            codeExp)

  let isSetDecl = CC.Declaration {
        CC.declarationName="isSet",
        CC.declarationType=CC.PrimitiveType mempty CC.CppBool
        }

  -- Make an empty call class.
  cls <- mkCallClass' (CC.PrimitiveType mempty CC.CppBool) [
    CC.IfElseBlock
      -- if (isSet && exp1 == val1 && exp2 == val2 ...) {
      (foldl eAnd (CC.SymbolExpression $ CC.declarationNameRef isSetDecl)
       $ toEquality <$> expsArg)
      --   return true;
      [CC.ReturnSt $ CC.LiteralBoolExpression False]
      -- } else { //set all expressions
      $ (toAssignment <$> expsArg)
      ++ [
      -- if (isSet)
      CC.IfElseBlock
        (CC.SymbolExpression $ CC.declarationNameRef isSetDecl)
         --   return true;
        [CC.ReturnSt $ CC.LiteralBoolExpression True]
        -- else { isSet = true;
        [CC.AssignmentSt
         $ CC.Assignment (CC.declarationNameRef isSetDecl)
         $ CC.LiteralBoolExpression True
        -- return false;}}
        , CC.ReturnSt $ CC.LiteralBoolExpression False]
      ]]

  -- Fill in the blanks
  return $ cls {
    CC.classConstructors=[
        -- Make sure we start with unset.
        CC.Constructor [(CC.declarationNameRef isSetDecl,
                         CC.LiteralBoolExpression False)] [] []],
    CC.classPrivateMembers=fmap CC.MemberVariable $ isSetDecl:(fst <$> expsArg)
    }
    where
      toAssignment (decl, codeExp) =
        CC.AssignmentSt $ CC.Assignment (CC.declarationNameRef decl) codeExp
      toEquality (decl, codeExp) =
        CC.E2ession (CC.Operator "==")
        codeExp
        $ CC.SymbolExpression $ CC.declarationNameRef decl
      eAnd = CC.E2ession $ CC.Operator "&&"

-- | Make a function class that builds the projection.
--
-- The expression could contain nulls in the case of the union but we
-- still need to know the type in order to operate.
projToFn :: forall c e s t n m .
         MonadClassBuilder c e s t n m
         => [(ShapeSym e s,Expr (Either CppType (ShapeSym e s)))]
         -> m (CC.Class CC.CodeSymbol)
projToFn pr = do
  sch <- projSchema pr >>= cppSchema
  primExpr <- projExpression pr
  primCls <- tellRecordCls sch
  let primSym = CC.classNameRef primCls
  let primType = CC.ClassType mempty [] primSym
  tellClassM $ mkCallClass primType primExpr

schemaProjectionClass :: MonadClassBuilder Identity e s t n m =>
                        CppSchema -> m (CC.Class CC.CodeSymbol)
schemaProjectionClass sch = tellClassM $ do
  retClsNam <- CC.classNameRef <$> tellRecordCls sch
  name <- CC.declarationNameRef . snd . runIdentity <$> getQueryDeclarations
  mkCallClass (CC.ClassType mempty [] retClsNam)
    $ CC.FunctionAp (CC.SimpleFunctionSymbol retClsNam) []
    $ CC.ObjectMember (CC.SymbolExpression name) . CC.Symbol . snd <$> sch

ashowCout :: AShow x => String -> x -> CC.Statement CC.CodeSymbol
ashowCout msg = CC.coutSt . return . CC.LiteralStringExpression . (msg ++) . ashow

queryRecord
  :: forall e s t n m .
  MonadCodeCheckpoint e s t n m
  => QueryShape e s
  -> m (CC.Class CC.CodeSymbol)
queryRecord qp = tellRecordCls =<< cppSchema (querySchema qp)
