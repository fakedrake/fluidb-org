{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# OPTIONS_GHC -Wno-unused-top-binds -Wno-type-defaults #-}

module Database.FluiDB.Running.Code
  ( sqlToSolution
  , insertAndRun
  , runCodeBuild
  , compileAndRunCppFile
  , planFrontier
  -- , realFileSize
  , runSingleQuery
  , NodeReport(..)
  ) where

import           Control.Monad.Free
import           Control.Monad.Writer
import qualified Data.HashSet                          as HS
import           Data.List.Extra
import qualified Data.List.NonEmpty                    as NEL
import           Data.Maybe
import           Database.FluiDB.BipartiteGraph
import           Database.FluiDB.CnfQuery
import           Database.FluiDB.Codegen.Build.Monads
import           Database.FluiDB.CppAst
import           Database.FluiDB.Debug
import           Database.FluiDB.Default
import           Database.FluiDB.HContT
import           Database.FluiDB.QueryPlan.Nodes
import           Database.FluiDB.QueryPlan.Transitions
import           Database.FluiDB.QuerySchema
import           Database.FluiDB.QuerySize

import           Database.FluiDB.Running.Classes
import           Database.FluiDB.Running.TpchValues
import           Database.FluiDB.Running.Types

import           Control.Monad.Except
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.State
import           Database.FluiDB.Algebra
import           Database.FluiDB.AShow
import           Database.FluiDB.Cluster
import           Database.FluiDB.Codegen.Build
import           Database.FluiDB.NodeContainers
import           Database.FluiDB.Optimizations
import           Database.FluiDB.QueryPlan             as QP
import           Database.FluiDB.Utils

reportGraph :: forall e s t n m .
                      (Hashables2 e s, Monad m, AShow e, AShow s) =>
                      CodeBuilderT e s t n m ()
#ifdef DUMP_GRAPH
reportGraph = do
  net <- clusterLiftCB $ lift2 $ gbPropNet <$> get
  gr <- evalStateT reprGraph $ GBState net mempty
  traceM $ "Graph: " ++ ashow (case gr of {Just x -> x;_ -> undefined})
#else
reportGraph = return ()
#endif

reportClusterConfig :: forall e s t n m .
                      (Hashables2 e s, Monad m, AShow e, AShow s) =>
                      CodeBuilderT e s t n m ()
#ifdef DUMP_CLUSTERS
reportClusterConfig = do
  ClusterConfig {..} <- getClusterConfig
  traceM "Begin Clusters"
  traceM $ ashow (cnfToClustMap, nrefToCnfs, trefToCnf)
  traceM "End Clusters"
#else
reportClusterConfig = return ()
#endif

queryDiff :: forall e s . (Eq e, Eq s) =>
            Query e s -> Query e s -> [(Query e s,Query e s)]
queryDiff = curry $ \case
  x@(Q2 o l r,Q2 o' l' r') -> if o /= o'
    then [x]
    else go o l l' r r'
  x@(Q1 o q,Q1 o' q') -> if o == o' then queryDiff q q' else [x]
  x@(Q0 s,Q0 s') -> if s == s' then [] else [x]
  x -> [x]
  where
    go = \case
      QJoin _ -> com
      QProd -> com
      _ -> \l l' r r' -> queryDiff l l' ++ queryDiff r r'
      where
        com :: Query e s
            -> Query e s
            -> Query e s
            -> Query e s
            -> [(Query e s,Query e s)]
        com l l' r r' = if maximum (maxDepth . snd <$> c) > maximum (maxDepth . snd <$> c')
                        then c'
                        else c
          where
          c = queryDiff l l' ++ queryDiff r r'
          c' = queryDiff l r' ++ queryDiff l r'

lengthF :: (Foldable f,Functor t,Foldable t) => Free (Compose t f) x -> Int
lengthF = \case
  Pure _ -> 1
  Free (Compose xs) -> sum $ foldr ((*) . lengthF) 1 <$> xs

type BacktrackMonad = []
-- type BacktrackMonad = ListT (State Int)
-- | try to build the c++ file
-- getQuerySolutionCpp
sqlToSolution :: forall a e s t n m .
                (Hashables2 e s,MonadFakeIO m,AShow e, AShow s) =>
                Query (PlanSym e s) (QueryPlan e s, s)
              -> (forall x . [x] -> m x) -- a lazy list to a monad we like.
              -> CodeBuilderT e s t n
              (HContT PlanSearchScore (GlobalUnMonad e s t n a) BacktrackMonad) a
              -> GlobalSolveT e s t n m a
sqlToSolution query serializeSols getSolution = do
  GlobalConf{globalExpTypeSymIso=symIso,globalQueryCppConf=cppConf} <- get
  -- traceM "Orig query:\n"
  -- traceM $ ashow $ first planSymOrig query
  qs <- optQueryPlan @e @s cppConf symIso query
  ioLogMsg $ "Opt queries: [" ++  show (lengthF qs) ++ "]\n"
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
  hoistGlobalSolveT (serializeSols . dissolve @BacktrackMonad)
    $ wrapTrace "Insert and run queries..."
    $ insertAndRun qs getSolution

-- | Insert a bunch of queries and then run
insertAndRun :: forall m a e s t n .
               (MonadHalt m, Hashable s, Hashables2 e s,
                AShow e, AShow s, MonadPlus m, BotMonad m,
                HValue m ~ PlanSearchScore) =>
               Free (Compose NEL.NonEmpty (Query e)) (s,QueryPlan e s)
             -> CodeBuilderT e s t n m a
             -> GlobalSolveT e s t n m a
insertAndRun queries postSolution = do
  gs <- get
  (ret, conf) <- joinExcept $ hoist (runCodeBuild . lift) $ solveQuery gs
  modify $ \gs' -> gs'{globalGCConfig=conf}
  return ret
  where
    solveQuery :: GlobalConf e s t n
               -> ExceptT (GlobalError e s t n)
               (CodeBuilderT e s t n m)
               (a, GCConfig t n)
    solveQuery GlobalConf{..} = do
      QueryCppConf{..} <- cbQueryCppConf <$> get
      when False $ do
        matNodes <- lift materializedNodes
        traceM $ "Mat nodes: " ++ ashow matNodes
        when (null matNodes) $ throwAStr "No ground truth"
      nOptqRef :: NodeRef n <- wrapTrace "inserting plans"
        $ lift3 $ insertQueryPlan literalType queries
      lift4 clearClustBuildCache
      lift $ reportGraph >> reportClusterConfig
      traceM $ "Commencing solution of node:" ++ ashow nOptqRef
      nodeNum <- length . rNodes . propNet <$> ask
      traceM $ "Total nodes:" ++ show nodeNum
      (qcost,conf) <- lift $ planLiftCB
        $ wrapTrace ("Solving node:" ++ show nOptqRef)
        $ do
          setNodeMaterialized nOptqRef
          ios <- fmap mconcat $ mapM transitionCost =<< dropReader get getTransitions
          gcost <- costNG nOptqRef
          return (gcost,ios)
      (,updateCosts nOptqRef qcost conf)
        <$> lift (local (const conf) $ postSolution)

reportSizes :: Monad m => NodeRef n -> PlanT t n m ()
reportSizes ref = do
  sizeMap :: RefMap n Int <-
    fmap (sum . fmap (fromMaybe (-1) . pageNum 4096) . fst) . nodeSizes <$> ask
  let reportNeighborSizes ref' = do
        sizeAssoc <- fmap (traverse2 (\x -> (x,) <$> x `refLU` sizeMap))
                    $ fmap2 (toNodeList . metaOpIn) $ findMetaOps ref'
        traceM $ printf "Neighbor sizes %n:" ref'
        traceM $ ashow sizeAssoc
        return $ fmap3 fst sizeAssoc
  traceM "Max sizes:"
  let sizes = take 10 $ sortOn (minus 0 . snd) $ refAssocs sizeMap
  forM_ sizes $ traceM . uncurry (printf "\t%n: %d pages")
  neigh <- toList3 <$> reportNeighborSizes ref
  mapM_ reportNeighborSizes neigh

materializedNodes :: (Hashables2 e s, Monad m) =>
                    CodeBuilderT e s t n m [(NodeState, NodeRef n)]
materializedNodes = lift5 $ do
  matIni <- fmap2 (Initial Mat,) $ nodesInState [Initial Mat]
  matConcN <- fmap2 (Concrete NoMat Mat,) $ nodesInState [Concrete NoMat Mat]
  matConcM <- fmap2 (Concrete Mat Mat,) $ nodesInState [Concrete Mat Mat]
  return $ matIni ++ matConcN ++ matConcM

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
    $ stateLayer mempty{gbPropNet=QP.propNet globalGCConfig}
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

compileAndRunCppFile
  :: forall e s t n m .
  MonadFakeIO m
  => FilePath
  -> FilePath
  -> GlobalSolveT e s t n m ()
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
  -- ioCmd "c++" command
  ioLogMsg $ "NOT: " ++ unwords ("c++":command)
  -- inDir (resourcesDir "tpch_data/") $ ioCmd exe []


#if 0

-- | Use the du unix tool to find out the sizes associated with a node
-- reference.
realFileSize :: forall e s t n m . (Hashables2 e s, MonadIO m) =>
               NodeRef n -> GlobalSolveT e s t n m (Maybe [TableSize])
realFileSize ref = do
  ls <- dropExcept (throwError . toGlobalError)
       $ dropState (globalClusterConfig <$> get,const $ return ())
       $ lookupClustersN ref
  case ls of
    [] -> error "wtf"
    (cnf, _):_ -> do
      sch <- runCodeBuild $ lift $ querySchema $ cnfOrigDEBUG cnf
      case schemaSize sch of
        Nothing -> error "wtf"
        Just schSize -> runMaybeT $ do
          fileSetM <- lift
                     $ runCodeBuild
                     $ lift
                     $ dropReader (lift3 get)
                     $ getNodeFile ref
          bytes <- case fileSetM of
            Nothing                -> error "the noderef is not materialized?"
            Just (DataFile f)      -> return <$> runDu f
            Just (DataAndSet f f') -> traverse runDu [f, f']
          return [tableSize' 4096 b schSize | b <- bytes]
  where
    runDu :: FilePath -> MaybeT (GlobalSolveT e s t n m) Int
    runDu file = MaybeT $ liftIO $ do
      exists <- fileExist file
      if exists
        then read . headErr . words <$> readProcess "/usr/bin/du" [file] ""
        else return Nothing

#endif

planFrontier :: [Transition t n] -> [NodeRef n]
planFrontier ts = toNodeList $ uncurry nsDifference $ foldl' go (mempty,mempty) ts
  where
    go :: (NodeSet n, NodeSet n)
       -> Transition t n -> (NodeSet n, NodeSet n)
    go (i,o) = \case
      Trigger i' _ o' -> (i <> fromNodeList i',o <> fromNodeList o')
      RTrigger i' _ o' -> (i <> fromNodeList i',o <> fromNodeList o')
      DelNode _ -> (i,o)

-- | Run a query and return the planned cost and the initial nodes used.
runSingleQuery :: (Hashables2 e s,AShow e,AShow s,ExpressionLike e,MonadFakeIO m) =>
                 Query (PlanSym e s) (QueryPlan e s, s)
               -> GlobalSolveT e s t n m ([Transition t n], String)
runSingleQuery query = do
  sqlToSolution query popSol $ do
    ts <- transitions . NEL.head . epochs <$> getGCState
    (ts,) <$> getQuerySolutionCpp
  where
    popSol :: Monad m => [x] -> m x
    popSol []    = error "No solution for query"
    popSol (x:_) = return x
