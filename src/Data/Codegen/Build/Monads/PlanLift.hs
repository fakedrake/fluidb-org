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
  ,SizeInferenceError(..)) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types.Clusters
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build.Monads.CodeBuilder
import           Data.Codegen.Build.Types
import           Data.Functor.Identity
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.QuerySize
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL

import           Data.Cluster.Propagators
import qualified Data.List.NonEmpty                    as NEL
import           Data.Query.QuerySchema.Types
import           Data.Utils.AShow
import           Data.Utils.Debug

-- | Lifts the plan monad to graph builder copying in the plan
-- computation any relevant information from the graph builder.
planLiftCB
  :: forall e s t n m a .
  (Monad m,Hashables2 e s)
  => PlanT t n m a
  -> CodeBuilderT' e s t n (PlanT t n m) (a,GCConfig t n)
planLiftCB plan = do
  graph <- lift4 $ gets gbPropNet
  cConf <- lift2 get
  gcConfE <- asks $ updateAll graph cConf
  case gcConfE of
    Left (Left e)  -> throwError $ CBESizeInferenceError e
    Left (Right e) -> lift4 $ throwError e
    Right gcConf   -> lift5 $ (,gcConf) <$> local (const gcConf) plan

updateConf
  :: (GCConfig t n -> a -> GCConfig t n)
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
updateAll graph cConf gcConf = do
  gcConf' <- updateSizes cConf $ updateGraph graph gcConf
  return $ updateIntermediates cConf gcConf'

updateGraph :: Bipartite t n -> GCConfig t n -> GCConfig t n
updateGraph graph gcConf = gcConf { propNet = graph }

updateSizes
  :: forall e s t n .
  Hashables2 e s
  => ClusterConfig e s t n
  -> GCConfig t n
  -> Either
    (Either (SizeInferenceError e s t n) (PlanningError t n))
    (GCConfig t n)
updateSizes cConf = updateConf (\gcConf s -> gcConf { nodeSizes = s }) $ do
  oldSizes <- lift2 $ asks nodeSizes
  let runMonads m = runExceptT $ (`execStateT` (cConf,oldSizes)) m
  unsizedNodes0 <- lift missingOrLowCertaintySizes
  (>>= either throwError (return . snd)) $ runMonads $ do
    unsizedNodes <- filterInterms unsizedNodes0
    -- Force all the queries first and then put them in the map so
    -- that calculating the size of one query does not change the
    -- value of another creating discrepancies between the map sent to
    -- the planner and the query shape map.
    dropState (gets fst,modify . first . const)
      $ mapM_ forceQueryShape unsizedNodes
    mapM putQuerySize unsizedNodes
  where
    filterInterms refs = (`filterM` refs) $ \ref ->
      dropReader (gets fst) (isIntermediateClust ref) >>= \case
        False -> return True
        True -> do
          modify (second $ refInsert ref (def,1))
          return False


-- | In (TableSize,Double) empty list means the node is an
-- intermediate.
putQuerySize
  :: forall e s t n m .
  (Hashables2 e s
  ,MonadError (SizeInferenceError e s t n) m
   -- Nothing in a node means we are currently looking up the node
  ,MonadState (ClusterConfig e s t n,RefMap n (TableSize,Double)) m)
  => NodeRef n
  -> m ()
putQuerySize ref = do
  shapeD <- dropReader (gets fst) $ getNodeShape ref
  shape <- maybe (throwAStr $ "Found empty shape for" <: (ref,shapeD)) return
    $ getDef shapeD
  modify $ second $ refInsert ref $ sizeToPair $ qpSize shape
  where
    sizeToPair QuerySize {..} = (qsTables,qsCertainty)

updateIntermediates
  :: Hashables2 e s => ClusterConfig e s t n -> GCConfig t n -> GCConfig t n
updateIntermediates cConf gcConf =
  gcConf
  { intermediates = fromNodeList
      $ join [clusterInterms x | x <- join $ toList $ qnfToClustMap cConf]
  }

-- | Find nodes that we are extremely unsure of.
missingOrLowCertaintySizes :: Monad m => PlanT t n m [NodeRef n]
missingOrLowCertaintySizes = do
  ns <- snd <$> allNodes
  sizes <- asks nodeSizes
  return $ (`filter` ns) $ \n ->
    maybe True ((< 0.3) . snd) $ refLU n sizes

getQnfM
  :: (MonadError (SizeInferenceError e s t n) m
     ,MonadReader (ClusterConfig e s t n) m)
  => NodeRef n
  -> m (NEL.NonEmpty (QNFQuery e s))
getQnfM k = do
  rMap <- asks nrefToQnfs
  case refLU k rMap of
    Nothing     -> throwError $ NoQnf k $ refKeys rMap
    Just []     -> throwError $ EmptyQnfList k
    Just (x:xs) -> return $ x NEL.:| xs


filterAlreadySized
  :: MonadState (RefMap n TableSize) m => [NodeRef n] -> m [NodeRef n]
filterAlreadySized refs = do
  rMap <- get
  return $ filter (not . (`refMember` rMap)) refs

onlyCC
  :: MonadState (ClusterConfig e s t n,RefMap n (Maybe ([TableSize],Double))) m
  => StateT (ClusterConfig e s t n) m a
  -> m a
onlyCC = dropState (gets fst,modify . first . const)
