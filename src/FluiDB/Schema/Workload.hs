{-# LANGUAGE CPP                    #-}
{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE InstanceSigs           #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE MultiWayIf             #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeApplications       #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-unused-top-binds -Wno-unused-imports -Wno-type-defaults #-}

-- | The entry point is `sqlToSolution'

module FluiDB.Schema.Workload
  (runWorkloadCpp
  ,runWorkloadEvals
  ,runSingleQuery) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Utils.Free
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.InsertQuery
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build
import           Data.Codegen.Build.Monads
import           Data.Codegen.Build.Monads.PlanLift
import           Data.Codegen.Build.Types
import           Data.CppAst.ExpressionLike
import           Data.Foldable
import qualified Data.HashSet                       as HS
import qualified Data.List.NonEmpty                 as NEL
import           Data.NodeContainers
import           Data.Proxy
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.Optimizations
import           Data.Query.Optimizations.Echo
import           Data.Query.QuerySchema.SchemaBase
import           Data.Query.QuerySchema.Types
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Solve
import           Data.QueryPlan.Transitions
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.HContT
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.MTL
import           Data.Utils.Unsafe
import           FluiDB.Classes
import           FluiDB.Runner.DefaultGlobal
import           FluiDB.Schema.TPCH.Values
import           FluiDB.Types
import           FluiDB.Utils


type ICodeBuilder e s t n a =
  CodeBuilderT
    e
    s
    t
    n
    (HContT PlanSearchScore (GlobalUnMonad e s t n a) [])
    a

lengthF :: (Foldable f,Functor t,Foldable t) => Free (Compose t f) x -> Int
lengthF = \case
  FreeT (Identity (Pure _))            -> 1
  FreeT (Identity (Free (Compose xs))) -> sum $ foldr ((*) . lengthF) 1 <$> xs

-- type BacktrackMonad = ListT (State Int)
-- | try to build the c++ file
-- getQuerySolutionCpp
sqlToSolution
  :: forall a e s t n m .
  (Hashables2 e s,MonadFakeIO m,AShow e,AShow s)
  => Query (ShapeSym e s) (QueryShape e s,s)
  -> (forall x . [x] -> m x) -- a lazy list to a monad we like.
  -> ICodeBuilder e s t n a
  -> GlobalSolveT e s t n m a
sqlToSolution query serializeSols getSolution = do
  GlobalConf{globalExpTypeSymIso=symIso,globalQueryCppConf=cppConf} <- get
  -- traceM "Orig query:\n"
  -- traceM $ ashow $ first shapeSymOrig query
  qs <- optQueryShape @e @s (literalType cppConf) symIso query
  -- SANITY CHECK
#if 0
  wrapTrace "Sanity checking..." $ do
    ioLogMsg ioOps $ "Opt queries: [size:" ++  show (lengthF qs) ++ "]\n"
    let toQnfs q = HS.fromList
          $ fmap fst
          $ either undefined id
          $ (`evalStateT` def)
          $ runListT
          $ toQNF (fmap2 snd . tableSchema cppConf) q
    let qnfs = toList
          $ fmap toQnfs $ fmap2 fst $ getCompose $ iterA interleaveComp qs
    forM_ (zip [1::Int ..] $ zip qnfs $ tail qnfs) $ \(_i,(bef,aft)) ->
      unless (bef == aft) $ throwAStr $ "UNEQUAL QNFa: "  ++ ashow (bef,aft)
#endif
  -- /SANITY CHECK
  hoistGlobalSolveT (serializeSols . dissolve @[])
    $ insertAndRun qs getSolution


-- |Assimilate all states and errors into the global state/error.
--
-- XXX: If m is Alternative we need to translate empty to a
-- GlobalError of the user's choice. Use Alt
runCodeBuild :: forall e s t n m x .
               Monad m =>
               ExceptT (GlobalError e s t n) (CodeBuilderT e s t n m) x
             -> GlobalSolveT e s t n m x
runCodeBuild x = do
  GlobalConf{..} <- get
  errLayer @(PlanningError t n)
    $ hoist (hoist (`runReaderT` globalGCConfig))
    -- Note that the gcState propnet here is overriden if we use
    -- planLift. (Set before or set after??)
    $ stateLayer (setNodeStatesToGCState globalMatNodes globalGCConfig def)
    $ stateLayer def {gbPropNet=propNet globalGCConfig}
    $ stateLayer globalClusterConfig
    $ errLayer @(ClusterError e s)
    $ stateLayer (emptyCBState globalQueryCppConf)
    $ errLayer @(CodeBuildErr e s t n)
    $ globalSolveWrap x
    where
      globalSolveWrap :: Monad m' =>
                        ExceptT (GlobalError e s t n) m' a
                      -> GlobalSolveT e s t n m' a
      globalSolveWrap = hoist lift

materializedNodes
  :: (Hashables2 e s,Monad m) => CodeBuilderT e s t n m [(NodeState,NodeRef n)]
materializedNodes = lift5 $ do
  matIni <- fmap2 (Initial Mat,) $ nodesInState [Initial Mat]
  matConcN <- fmap2 (Concrete NoMat Mat,) $ nodesInState [Concrete NoMat Mat]
  matConcM <- fmap2 (Concrete Mat Mat,) $ nodesInState [Concrete Mat Mat]
  return $ matIni ++ matConcN ++ matConcM

insertAndRun
  :: forall m a e s t n .
  (MonadHalt m
  ,Hashable s
  ,Hashables2 e s
  ,AShow e
  ,AShow s
  ,MonadPlus m
  ,BotMonad m
  ,HValue m ~ PlanSearchScore)
  => Free (Compose NEL.NonEmpty (TQuery e)) (s,QueryShape e s)
  -> CodeBuilderT e s t n m a
  -> GlobalSolveT e s t n m a
insertAndRun queries postSolution = do
  (ret,conf) <- joinExcept $ hoist (runCodeBuild . lift) $ do
    ref <- insertQueries queries
    lift $ matNode postSolution ref
  modify $ \gs' -> gs' { globalGCConfig = conf }
  case ret of
    Left e  -> throwError e
    Right x -> return x

insertQueries
  :: (Hashables2 e s, Monad m)
  => Free (Compose NEL.NonEmpty (TQuery e)) (s,QueryShape e s)
  -> ExceptT (GlobalError e s t n) (CodeBuilderT e s t n m) (NodeRef n)
insertQueries queries = do
  QueryCppConf {..} <- gets cbQueryCppConf
  when False $ do
    matNodes <- lift materializedNodes
    traceM $ "Mat nodes: " ++ ashow matNodes
    when (null matNodes) $ throwAStr "No ground truth"
  nOptqRef :: NodeRef n <- lift3 $ insertQueryForest literalType queries
  lift4 clearClustBuildCache
  -- lift $ reportGraph >> reportClusterConfig/
  nodeNum <- asks (length . nNodes . propNet)
  traceM
    $ printf
      "Total nodes: %d, solving node: %n, opt DAG size: %d"
      nodeNum
      nOptqRef
      (lengthF queries)
  return nOptqRef


-- | Materializ node. If an error occurs return it as Left in order to
-- avoid losing the entire configuration, it will invariably be useful
-- for error reporting.
matNode
  :: (HValue m ~ PlanSearchScore
     ,Monad m
     ,Hashables2 e s
     ,MonadHalt m
     ,BotMonad m
     ,MonadPlus m)
  => CodeBuilderT e s t n m a
  -> NodeRef n
  -> CodeBuilderT e s t n m (Either (GlobalError e s t n) a,GCConfig t n)
matNode postSolution nOptqRef = do
  (errM
    ,conf) <- planLiftCB $ reifyError (setNodeMaterialized nOptqRef) >>= \case
    Left e   -> return $ Just $ toGlobalError e
    Right () -> return Nothing
  let conf' = pushHistory nOptqRef conf
  case errM of
    Just e  -> return (Left e,conf)
    Nothing -> (,conf') . Right <$> local (const conf) postSolution

reifyError :: MonadError a m => m b -> m (Either a b)
reifyError m = fmap Right m `catchError` (return . Left)

planFrontier :: [Transition t n] -> [NodeRef n]
planFrontier
  ts = toNodeList $ uncurry nsDifference $ foldl' go (mempty,mempty) ts
  where
    go :: (NodeSet n,NodeSet n) -> Transition t n -> (NodeSet n,NodeSet n)
    go (i,o) = \case
      Trigger i' _ o'  -> (i <> fromNodeList i',o <> fromNodeList o')
      RTrigger i' _ o' -> (i <> fromNodeList i',o <> fromNodeList o')
      DelNode _        -> (i,o)

type CppCode = String
runSingleQuery
  :: (Hashables2 e s,AShow e,AShow s,ExpressionLike e,MonadFakeIO m)
  => Query (ShapeSym e s) (QueryShape e s,s)
  -> GlobalSolveT e s t n m ([Transition t n],CppCode)
runSingleQuery query = sqlToSolution query popSol $ do
  ts <- transitions . NEL.head . epochs <$> getGCState
  (ts,) <$> getQuerySolutionCpp
  where
    popSol :: Monad m => [x] -> m x
    popSol []    = error "No solution for query"
    popSol (x:_) = return x


-- | Iterate over the queryes
forEachQuery
  :: forall e s t n m q a .
  (MonadFail m,AShowV e,AShowV s,DefaultGlobal e s t n m q)
  => (GlobalConf e s t n -> GlobalConf e s t n)
  -> [q]
  -> (Int -> Query (ShapeSym e s) (QueryShape e s,s) -> GlobalSolveT e s t n m a)
  -> m [a]
forEachQuery modGQnf qios m =
  runGlobalSolve
    (modGQnf $ defGlobalConf (Proxy :: Proxy m) qios)
    (\x -> fail $ "No solution found: " ++ ashow x)
  $ forM (zip [1 ..] qios)
  $ \(i,qio) -> do
    query <- getIOQuery qio
    m i query


-- | Return the planned (not the actually run) cost and the nodes used
-- for materialized
runWorkloadCpp
  :: forall e s t n m q .
  (MonadFail m,AShowV e,AShowV s,DefaultGlobal e s t n m q)
  => (GlobalConf e s t n -> GlobalConf e s t n)
  -> [q]
  -> m [([(Transition t n,Cost)],[NodeRef n])]
runWorkloadCpp modGQnf qios = forEachQuery modGQnf qios $ \i query -> do
  (trigs,cppCode) <- runSingleQuery query
  gcc :: GCConfig t n <- gets getGlobalConf
  costTrigs <- either (throwError . toGlobalError) return
    $ dropReader (return gcc)
    $ mapM (\t -> (t,) <$> transitionCost t) trigs
  ioWriteFile ioOps (cppFile i) cppCode
  inFn <- existing $ cppFile i
  let command =
        ["-std=c++17"
        ,"-g"
        ,"-I" ++ resourcesDir "include"
        ,resourcesDir "include/book.cc"
        ,inFn
        ,"-o"
        ,"-"]
  -- ioCmd "c++" command
  ioLogMsg ioOps $ "NOT: " ++ unwords ("c++" : command)
  return (costTrigs,planFrontier trigs)

-- | Run a workload turning it into evals.
--
-- If all goes well: return (lenght k) === lenght <$> runWorkloadEvals k
runWorkloadEvals
  :: forall e s t n m q .
  (MonadFail m,AShowV e,AShowV s,DefaultGlobal e s t n m q)
  => (GlobalConf e s t n -> GlobalConf e s t n)
  -> [q]
  -> m [[Evaluation e s t n [QNFQuery e s]]]
runWorkloadEvals modGConf qs = forEachQuery modGConf qs $ \_i query ->
  sqlToSolution query (return . headErr)
  $ dropReader (lift2 askStates) getEvaluations
  where
    askStates = gets (,,,) <*> lift2 get <*> lift3 get <*> lift3 ask
