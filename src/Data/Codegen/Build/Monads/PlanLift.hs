{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.Monads.PlanLift
  (planLiftCB
  ,SizeInferenceError(..)
  ,querySize
  ,updateSizes
  ,missingSizes) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.ClusterConfig
import           Data.Cluster.FoldPlans
import           Data.Cluster.Types.Clusters
import           Data.Cluster.Types.Monad
import           Data.CnfQuery.Types
import           Data.Codegen.Build.Monads.CodeBuilder
import           Data.Codegen.Build.Types
import           Data.Functor.Identity
import           Data.NodeContainers
import           Data.Query.QuerySize
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL

import qualified Data.List.NonEmpty                    as NEL


-- | Lifts the plan monad to graph builder copying in the plan
-- computation any relevant information from the graph builder.
planLiftCB :: forall e s t n m a .
             (Monad m, Hashables2 e s) =>
             PlanT t n m a
           -> CodeBuilderT' e s t n (PlanT t n m) (a, GCConfig t n)
planLiftCB plan = do
  graph <- lift4 $ gbPropNet <$> get
  cConf <- lift2 get
  gcConfE <- asks (updateAll graph cConf)
  case gcConfE of
    Left (Left e)  -> throwError $ CBESizeInferenceError e
    Left (Right e) -> lift4 $ throwError e
    Right gcConf   -> lift5 $ (,gcConf) <$> local (const gcConf) plan

updateConf :: (GCConfig t n -> a -> GCConfig t n)
           -> ExceptT (SizeInferenceError e s t n) (PlanT t n Identity) a
           -> GCConfig t n
           -> Either
           (Either (SizeInferenceError e s t n) (PlanningError t n))
           (GCConfig t n)
updateConf putVal mkValM conf =
  putVal conf <$> case runIdentity $ runPlanT' def conf $ runExceptT mkValM of
    Left e          -> Left $ Right e
    Right (Left e)  -> Left $ Left e
    Right (Right x) -> Right x

updateAll
  :: Hashables2 e s
  => Bipartite t n
  -> ClusterConfig e s t n
  -> GCConfig t n
  -> Either
    (Either (SizeInferenceError e s t n) (PlanningError t n))
    (GCConfig t n)
updateAll graph cConf =
  return . updateIntermediates cConf
  <=< updateSizes cConf
  <=< return . updateGraph graph

updateGraph :: Bipartite t n -> GCConfig t n -> GCConfig t n
updateGraph graph gcConf = gcConf{propNet=graph}

updateSizes
  :: forall e s t n .
  Hashables2 e s
  => ClusterConfig e s t n
  -> GCConfig t n
  -> Either
    (Either (SizeInferenceError e s t n) (PlanningError t n))
    (GCConfig t n)
updateSizes cConf =
  updateConf (\gcConf s -> gcConf{nodeSizes=s}) $ do
    unsizedNodes <- lift missingSizes
    oldSizes <- lift2 $ asks nodeSizes
    let runMonads m = runExceptT $ (`execStateT` (cConf,Just <$> oldSizes)) m
    runMonads (filterInterms unsizedNodes >>= mapM (fmap fst . querySize))
      >>= \case
      Left e -> throwError e
      Right (_,rmap) -> lift $ traverse
        (maybe
         (error "planLift1 (in querySize) did not finish the recursion")
         return) rmap

updateIntermediates :: Hashables2 e s =>
                      ClusterConfig e s t n
                    -> GCConfig t n
                    -> GCConfig t n
updateIntermediates cConf gcConf =
  gcConf
  { intermediates = fromNodeList
      $ join [clusterInterms x | x <- join $ toList $ cnfToClustMap cConf]
  }


missingSizes :: Monad m => PlanT t n m [NodeRef n]
missingSizes = do
  ns <- snd <$> allNodes
  sizes <- asks nodeSizes
  return $ (`filter` ns) $ \n
    -> maybe True ((< 0.5) . snd) $ refLU n sizes

getCnfM :: (MonadError (SizeInferenceError e s t n) m,
           MonadReader (ClusterConfig e s t n) m) =>
           NodeRef n -> m (NEL.NonEmpty (CNFQuery e s))
getCnfM k = do
  rMap <- asks nrefToCnfs
  case refLU k rMap of
    Nothing     -> throwError $ NoCnf k $ refKeys rMap
    Just []     -> throwError $ EmptyCnfList k
    Just (x:xs) -> return $ x NEL.:| xs


filterAlreadySized
  :: MonadState (RefMap n [TableSize]) m => [NodeRef n] -> m [NodeRef n]
filterAlreadySized refs = do
  rMap <- get
  return $ filter (not . (`refMember` rMap)) refs

filterInterms
  :: (Hashables2 e s
     ,MonadState
        (ClusterConfig e s t n,RefMap n (Maybe ([TableSize],Double)))
        m)
  => [NodeRef n]
  -> m [NodeRef n]
filterInterms refs = (`filterM` refs) $ \ref ->
  dropReader (gets fst) (isIntermediateClust ref) >>= \case
    True  -> modify (second $ refInsert ref $ Just ([],1)) >> return False
    False -> return True

onlyCC :: (MonadState
          (ClusterConfig e s t n, RefMap n (Maybe ([TableSize],Double)))
          m) =>
         StateT (ClusterConfig e s t n) m a -> m a
onlyCC = dropState (gets fst,modify . first . const)
