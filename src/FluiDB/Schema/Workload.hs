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

module FluiDB.Schema.Workload
  (runWorkload) where

import Data.Foldable
import Data.Query.QuerySchema.SchemaBase
import Data.Bifunctor
import FluiDB.Schema.TPCH.Values
import Data.Codegen.Build
import Data.Codegen.Build.Monads.PlanLift
import Data.Utils.MTL
import Data.QueryPlan.Transitions
import Data.QueryPlan.Solve
import Data.Cluster.InsertQuery
import Control.Monad.Reader
import Data.QueryPlan.Nodes
import Data.QueryPlan.Types
import Data.BipartiteGraph
import Data.Cluster.Types.Monad
import Control.Monad.Morph
import Control.Monad.Except
import Data.Utils.Debug
import Data.Query.Optimizations
import Control.Monad.Free
import Data.Utils.Compose
import qualified Data.HashSet as HS
import Data.Utils.HContT
import Data.Codegen.Build.Monads
import Data.Query.QuerySchema.Types
import qualified Data.List.NonEmpty as NEL
import Data.CppAst.ExpressionLike
import FluiDB.Classes
import Data.Utils.Hashable
import Data.Codegen.Build.Types
import Data.Utils.Functors
import Data.CnfQuery.Build
import Data.Utils.ListT
import Data.Utils.Default
import Control.Monad.State
import FluiDB.Utils
import Data.Utils.Unsafe
import Data.Proxy
import FluiDB.Runner.DefaultGlobal
import Data.NodeContainers
import Data.QueryPlan.Types
import Data.CnfQuery.Types
import Data.Query.Algebra
import FluiDB.Types
import Data.Utils.AShow


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
  Pure _ -> 1
  Free (Compose xs) -> sum $ foldr ((*) . lengthF) 1 <$> xs

-- type BacktrackMonad = ListT (State Int)
-- | try to build the c++ file
-- getQuerySolutionCpp
sqlToSolution
  :: forall a e s t n m .
  (Hashables2 e s,MonadFakeIO m,AShow e,AShow s)
  => Query (PlanSym e s) (QueryPlan e s,s)
  -> (forall x . [x] -> m x) -- a lazy list to a monad we like.
  -> ICodeBuilder e s t n a
  -> GlobalSolveT e s t n m a
sqlToSolution query serializeSols getSolution = do
  GlobalConf{globalExpTypeSymIso=symIso,globalQueryCppConf=cppConf} <- get
  -- traceM "Orig query:\n"
  -- traceM $ ashow $ first planSymOrig query
  qs <- optQueryPlan @e @s (literalType cppConf) symIso query
  logMsg $ "Opt queries: [" ++  show (lengthF qs) ++ "]\n"
  -- SANITY CHECK
  when False $ wrapTrace "Sanity checking..." $ do
    let toCnfs q = HS.fromList
          $ fmap fst
          $ either undefined id
          $ (`evalStateT` def)
          $ runListT
          $ toCNF (fmap2 snd . tableSchema cppConf) q
    let cnfs = toList
          $ fmap toCnfs $ fmap2 fst $ getCompose $ iterA interleaveComp qs
    forM_ (zip [1::Int ..] $ zip cnfs $ tail cnfs) $ \(_i,(bef,aft)) ->
      unless (bef == aft) $ throwAStr $ "UNEQUAL CNFa: "  ++ ashow (bef,aft)
  -- /SANITY CHECK
  hoistGlobalSolveT (serializeSols . dissolve @[])
    $ wrapTrace "Insert and run queries..."
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
    $ stateLayer mempty{gbPropNet=propNet globalGCConfig}
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

materializedNodes :: (Hashables2 e s, Monad m) =>
                    CodeBuilderT e s t n m [(NodeState, NodeRef n)]
materializedNodes = lift5 $ do
  matIni <- fmap2 (Initial Mat,) $ nodesInState [Initial Mat]
  matConcN <- fmap2 (Concrete NoMat Mat,) $ nodesInState [Concrete NoMat Mat]
  matConcM <- fmap2 (Concrete Mat Mat,) $ nodesInState [Concrete Mat Mat]
  return $ matIni ++ matConcN ++ matConcM

insertAndRun :: forall m a e s t n .
               (MonadHalt m, Hashable s, Hashables2 e s,
                AShow e, AShow s, MonadPlus m, BotMonad m,
                HValue m ~ PlanSearchScore) =>
               Free (Compose NEL.NonEmpty (Query e)) (s,QueryPlan e s)
             -> CodeBuilderT e s t n m a
             -> GlobalSolveT e s t n m a
insertAndRun queries postSolution = do
  (ret,conf) <- joinExcept $ hoist (runCodeBuild . lift) $ solveQuery
  modify $ \gs' -> gs' { globalGCConfig = conf }
  return ret
  where
    solveQuery
      :: ExceptT
        (GlobalError e s t n)
        (CodeBuilderT e s t n m)
        (a,GCConfig t n)
    solveQuery = do
      QueryCppConf {..} <- cbQueryCppConf <$> get
      when False $ do
        matNodes <- lift materializedNodes
        traceM $ "Mat nodes: " ++ ashow matNodes
        when (null matNodes) $ throwAStr "No ground truth"
      nOptqRef :: NodeRef n <- wrapTrace "inserting plans"
        $ lift3
        $ insertQueryPlan literalType queries
      lift4 clearClustBuildCache
      -- lift $ reportGraph >> reportClusterConfig
      traceM $ "Commencing solution of node:" ++ ashow nOptqRef
      nodeNum <- length . rNodes . propNet <$> ask
      traceM $ "Total nodes:" ++ show nodeNum
      (_qcost,conf)
        <- lift $ planLiftCB $ wrapTrace ("Solving node:" ++ show nOptqRef) $ do
          setNodeMaterialized nOptqRef
          ios <- fmap mconcat
            $ mapM transitionCost =<< dropReader get getTransitions
          return ios
      (,conf) <$> lift (local (const conf) $ postSolution)


-- | Run a query and return the planned cost and the initial nodes used.
runSingleQuery
  :: (Hashables2 e s,AShow e,AShow s,ExpressionLike e,MonadFakeIO m)
  => Query (PlanSym e s) (QueryPlan e s,s)
  -> GlobalSolveT e s t n m ([Transition t n],String)
runSingleQuery query = do
  sqlToSolution query popSol $ do
    ts <- transitions . NEL.head . epochs <$> getGCState
    (ts,) <$> getQuerySolutionCpp
  where
    popSol :: Monad m => [x] -> m x
    popSol [] = error "No solution for query"
    popSol (x:_) = return x

compileAndRunCppFile :: forall e s t n m . MonadFakeIO m =>
                       FilePath -> FilePath -> GlobalSolveT e s t n m ()
compileAndRunCppFile cpp exe = do
  inFn <- existing cpp
  let command = [
        "-std=c++17"
        , "-g"
        , "-I" ++ resourcesDir "include"
        , resourcesDir "include/book.cc"
        , inFn
        , "-o"
        , exe]
  -- cmd "c++" command
  logMsg $ "NOT: " ++ unwords ("c++":command)
  -- inDir (resourcesDir "tpch_data/") $ cmd exe []

planFrontier :: [Transition t n] -> [NodeRef n]
planFrontier
  ts = toNodeList $ uncurry nsDifference $ foldl' go (mempty,mempty) ts
  where
    go :: (NodeSet n,NodeSet n) -> Transition t n -> (NodeSet n,NodeSet n)
    go (i,o) = \case
      Trigger i' _ o' -> (i <> fromNodeList i',o <> fromNodeList o')
      RTrigger i' _ o' -> (i <> fromNodeList i',o <> fromNodeList o')
      DelNode _ -> (i,o)

-- | Return the planned (not the actually run) cost and the nodes used
-- for materialized
runWorkload
  :: forall e s t n m q .
  (MonadFail m,AShowV e,AShowV s,DefaultGlobal e s t n m q)
  => (GlobalConf e s t n -> GlobalConf e s t n)
  -> [q]
  -> m [([(Transition t n,Cost)],[NodeRef n])]
runWorkload modGCnf qios =
  mRun
    (modGCnf $ defGlobalConf (Proxy :: Proxy m) qios)
    (\x -> fail $ "No solution found: " ++ ashow x)
  $ forM (zip [1 ..] qios)
  $ \(i,qio) -> do
    query <- getIOQuery qio
    (trigs,cppCode) <- runSingleQuery query
    gcc :: GCConfig t n <- getGlobalConf <$> get
    costTrigs <- either (throwError . toGlobalError) return
      $ dropReader (return gcc)
      $ mapM (\t -> (t,) <$> transitionCost t) trigs
    writeFileFake (cppFile i) cppCode
    compileAndRunCppFile (cppFile i) $ exeFile i
    return (costTrigs,planFrontier trigs)

getTpchQuery :: SqlTypeVars e s t n => Int -> IO (Query e s)
getTpchQuery q =
  fmap (bimap planSymOrig snd)
  $ mRun
    (defGlobalConf (Proxy :: Proxy IO) ([] :: [Int]))
    (\x -> fail $ "Couldn't get query: " ++ ashow x)
  $ getIOQuery q
