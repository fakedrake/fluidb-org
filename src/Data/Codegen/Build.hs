{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE KindSignatures        #-}
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

module Data.Codegen.Build
  ( CodeBuilderT
  , clusterLiftCB
  , getCppCode
  , CBState(..)
  , QueryCppConf(..)
  , CodeBuildErr(..)
  , Evaluation(..)
  , getEvaluations
  , getQueryCppConf
  , runSoftCodeBuilder
  , queryRecord
  , throwCodeErr
  , MonadCodeError
  , MonadCodeBuilder
  , tellFunctionM
  , tellInclude
  , emptyCBState
  , getCppProgram
  , exprCppType
  , evalQueryEnv
  , getQuerySolutionCpp
  , evaluationValue
  , QueryShape
  , ShapeSym
  ) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types
import           Data.Codegen.Build.Classes
import           Data.Codegen.Build.Constructors
import           Data.Codegen.Build.Monads
import           Data.Codegen.Build.UpdateMatShapes
import           Data.Codegen.TriggerCode
import qualified Data.CppAst                        as CC
import           Data.Either
import qualified Data.HashSet                       as HS
import           Data.List
import qualified Data.List.NonEmpty                 as NEL
import           Data.Maybe
import           Data.Monoid
import           Data.NodeContainers
import           Data.QnfQuery.BuildUtils
import           Data.QnfQuery.Types
import           Data.Query.QuerySchema
import           Data.QueryPlan.Transitions
import           Data.QueryPlan.Types
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.MTL
import           Data.Utils.Tup
import           GHC.Generics
import           Prelude                            hiding (exp)

data Evaluation e s t n q
  = ForwardEval (AnyCluster e s t n) (Tup2 [NodeRef n]) q
  | ReverseEval (AnyCluster e s t n) (Tup2 [NodeRef n]) q
  | Delete (NodeRef n) q
  deriving (Eq,Functor,Generic)
instance (AShow s,AShow e,Hashables2 e s,AShow q) => AShow (Evaluation e s t n q)
evaluationValue :: Evaluation e s t n q -> q
evaluationValue = \case
  ForwardEval _ _ q -> q
  ReverseEval _ _ q -> q
  Delete _ q        -> q

-- | The code of each plan.
getCppCode
  :: forall e s (t :: *) (n :: *) m .
  (AShow2 e s,CC.ExpressionLike e,Monad m,Hashables2 e s)
  => CodeBuilderT e s t n m [CC.Statement CC.CodeSymbol]
getCppCode = runSoftCodeBuilder $ forEachEpoch $ do
  ep <- NEL.head . epochs <$> lift getGCState
  ts <- lift $ dropReader (lift2 get) $ bundleTransitions $ transitions ep
  fmap mconcat $ forM ts $ \case
    ForwardTransitionBundle ts clust -> do
      lift $ mapM_ internalSyncShape $ clusterInputs clust
      void $ lift $ triggerCluster clust
      lift $ mapM_ promoteNodeShape $ clusterOutputs clust
      mkCodeBlock ts clust ForwardTrigger $ triggerCode ts
    ReverseTransitionBundle ts clust -> do
      lift $ mapM_ internalSyncShape $ clusterOutputs clust
      void $ lift $ triggerCluster clust
      lift $ mapM_ promoteNodeShape $ clusterInputs clust
      mkCodeBlock ts clust ReverseTrigger $ revTriggerCode ts
    DeleteTransitionBundle n -> do
      void $ lift $ demoteNodeShape n
      fileM <- dropReader (lift getClusterConfig) $ getNodeFile n
      queryView <- dropState (lift getClusterConfig,const $ return ())
        $ (maybe (throwError $ NodeNotFoundN n) return =<<)
        $ fmap listToMaybe
        $ fmap2 qnfOrigDEBUG
        $ getNodeQnfN n
      let comment = CC.Comment $ "Delete: " ++ ashow queryView
      let cout = ashowCout "Delete: " queryView
      dropReader (lift getClusterConfig) $ delNodeFile n
      case fileM of
        Nothing   -> return []
        Just file -> [comment,cout] `andThen` delCode file

andThen
  :: Monad m
  => [CC.Statement CC.CodeSymbol]
  -> m (CC.Statement CC.CodeSymbol)
  -> m [CC.Statement CC.CodeSymbol]
andThen stmts finalM = do
  final <- finalM
  return $ stmts ++ [final]

mkCodeBlock
  :: forall e s t n m .
  (CC.ExpressionLike e,AShow2 e s,Monad m,Hashables2 e s)
  => Tup2 [NodeRef n]
  -> AnyCluster e s t n
  -> Direction
  -> (AnyCluster e s t n
      -> ReaderT
        (ClusterConfig e s t n,GCState t n)
        (StateT Int (CodeBuilderT e s t n m))
        (CC.Statement CC.CodeSymbol))
  -> StateT Int (CodeBuilderT e s t n m) [CC.Statement CC.CodeSymbol]
mkCodeBlock _io clust triggerDirection internalMkCode = do
  let trigOps =
        fmap (bimap (nub . fmap2 shapeSymOrig) (nub . fmap2 shapeSymOrig))
        $ fst
        $ primaryNRef clust
  let comment = CC.Comment $ show triggerDirection ++ ": " ++ ashow trigOps
  let cout = ashowCout (show triggerDirection ++ ": ") trigOps
  [comment,cout]
    `andThen` dropReader
      (lift $ (,) <$> getClusterConfig <*> getGCState)
      (internalMkCode clust)

-- Cycle through the epoch heads. The latest epoch and transition
-- is at the head of the list.
forEachEpoch
  :: forall e s t n m a tr .
  (AShow e
  ,AShow s
  ,CC.ExpressionLike e
  ,Monad m
  ,Eq e
  ,Monoid a
  ,MonadTrans tr
  ,Monad (tr (CodeBuilderT e s t n m)))
  => tr (CodeBuilderT e s t n m) a
  -> tr (CodeBuilderT e s t n m) a
forEachEpoch pl = do
  eps <- liftPlanT $ gets $ reverse . toList . nelTails . epochs
  fmap mconcat $ forM eps $ \ep -> do
    liftPlanT $ modify (\gcs -> gcs { epochs = ep })
    pl
  where
    liftPlanT = lift2 . lift4
    nelTails :: NEL.NonEmpty x -> [NEL.NonEmpty x]
    nelTails a@(_ NEL.:| []) = [a]
    nelTails a@(_ NEL.:| (x:xs)) =
      a : nelTails (x NEL.:| xs)


cppReportPerf :: [CC.Statement CC.CodeSymbol]
cppReportPerf = [
  CC.FunctionAp "perf_report"
                ]
-- | The main function.
getCppMain
  :: (AShow e,AShow s,CC.ExpressionLike e,Monad m,Eq e)
  => [CC.Statement CC.CodeSymbol]
  -> CodeBuilderT e s t n m (CC.Function CC.CodeSymbol)
getCppMain body = do
  let ret = CC.ReturnSt $ CC.LiteralIntExpression 0
  return CC.Function {
    functionName="main",
    functionType=CC.PrimitiveType mempty CC.CppInt,
    functionBody=body ++ cppReportPerf ++ [ret],
    functionArguments=[],
    functionConstMember=False
    }

-- | Get the entire program as a string.
getCppProgram
  :: forall e s t n m .
  (AShow e,AShow s,CC.ExpressionLike e,Monad m,Eq e)
  => [CC.Statement CC.CodeSymbol]
  -> CodeBuilderT e s t n m String
getCppProgram body = do
  main <- getCppMain body
  CBState {..} <- getCBState
  classBlock <- toCodeBlock CC.classNameRef cbClasses
  functionBlock <- toCodeBlock CC.functionNameRef cbFunctions
  let fHs = intercalate "\n" . fmap CC.toCodeIndent . HS.toList
  return
    $ fHs cbIncludes
    ++ classBlock
    ++ "\n\n\n"
    ++ functionBlock
    ++ "\n\n\n"
    ++ CC.toCodeIndent main
  where
    toCodeBlock
      :: (Traversable t'
         ,Show (t' ())
         ,Hashable (t' CC.CodeSymbol)
         ,Eq (t' CC.CodeSymbol)
         ,CC.CodegenIndent (t' CC.CodeSymbol) String)
      => (t' CC.CodeSymbol -> CC.Symbol CC.CodeSymbol)
      -> CC.CodeCache t'
      -> CodeBuilderT e s t n m String
    toCodeBlock f =
      toCodeBuild
      . bimap toCodeBl toCodeBl
      . CC.sortDefs (CC.runSymbol . f)
      . CC.runCodeCache
      where
        toCodeBl = intercalate "\n\n" . fmap CC.toCodeIndent
        toCodeBuild = \case
          Left xs  -> throwError $ CircularReference xs
          Right xs -> return xs

-- | Physical plans
getQuerySolutionCpp
  :: forall e s t n m .
  (AShow2 e s,CC.ExpressionLike e,Monad m,Hashables2 e s)
  => CodeBuilderT e s t n m String
getQuerySolutionCpp = do
  tellInclude $ CC.LibraryInclude "codegen.hh"
  tellInclude $ CC.LibraryInclude "array"
  tellInclude $ CC.LibraryInclude "string"
  body <- getCppCode
  getCppProgram body

getEvaluations
  :: forall e s t n m err .
  (MonadReader (ClusterConfig e s t n,GBState t n,GCState t n,GCConfig t n) m
  ,MonadAShowErr e s err m
  ,Hashables2 e s)
  => m [Evaluation e s t n [QNFQuery e s]]
getEvaluations = runListT $ mkListT getCleanBundles >>= \case
  ForwardTransitionBundle io c -> ForwardEval c io <$> getQueriesC c
  ReverseTransitionBundle io c -> ReverseEval c io <$> getQueriesC c
  DeleteTransitionBundle n     -> Delete n <$> getQNFs n
  where
    getCleanBundles :: m [TransitionBundle e s t n]
    getCleanBundles = do
      trns <- reverse <$> dropReader (asks trd4) getTransitions
      dropReader (asks fst4) $ bundleTransitions trns
    getQNFs :: NodeRef n -> ListT m [QNFQuery e s]
    getQNFs
      ref = lift $ dropState (asks fst4,const $ return ()) $ lookupQnfN ref
    getQueriesC :: AnyCluster e s t n -> ListT m [QNFQuery e s]
    getQueriesC = dropState (asks fst4,const $ return ()) . getQueriesFromClust
