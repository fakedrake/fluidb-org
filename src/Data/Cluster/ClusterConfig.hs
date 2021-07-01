{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Data.Cluster.ClusterConfig
  (getClustersNonInput
  ,getNodeQnfN
  ,registerClusterInput
  ,mkNodeFromQnfT
  ,lookupClustersX
  ,lookupClustersN
  ,lookupClusterT
  ,replaceCluster
  ,lookupClusters
  ,matchNode
  ,linkQnfClust
  ,linkNRefQnf
  ,clearNRefQnfs
  ,linkTRefQnf
  ,clearTRefQnf
  ,lookupQnfN
  ,mkNodeFromQnf
  ,mkNodeFormQnfNUnsafe
  ,isIntermediateClust) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bipartite
import           Data.Cluster.Types
import qualified Data.HashMap.Strict  as HM
import           Data.List
import           Data.Maybe
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL

-- destruct c = (clusterInputs c,clusterOutputs c)
replaceCluster
  :: (MonadError err m
     ,AShowError e s err
     ,Hashables2 e s
     ,MonadState (ClusterConfig e s t n) m)
  => AnyCluster e s t n
  -> AnyCluster e s t n
  -> m ()
replaceCluster cold cnew = do
  qnfsT <- mapM luQNFT $ fst $ allNodeRefsAnyClust cold
  qnfsN <- fmap join $ mapM luQNFN $ snd $ allNodeRefsAnyClust cold
  -- XXX We are finding qnfs but not props!!
  props <- gets (HM.lookup cold . qnfPropagators) >>= \case
    Nothing -> throwAStr $ "Not in update map: " ++ ashow (cold,cnew,qnfsN)
    Just x  -> return x
  modify $ \cc -> cc
    { qnfPropagators =
        HM.insert cnew props $ HM.delete cold $ qnfPropagators cc
    }
  forM_ (nub $ qnfsN ++ qnfsT) $ \qnf -> modify $ \cc -> cc
    { qnfToClustMap = HM.adjust ((cnew :) . filter (/= cold)) qnf
        $ qnfToClustMap cc
    }
  where
    luQNFT t = gets (refLU t . trefToQnf) >>= \case
      Just qnf -> return qnf
      Nothing  -> throwAStr $ "Couldn't find qnf for t node: " ++ show t
    luQNFN n = gets (refLU n . nrefToQnfs) >>= \case
      Just qnfs -> return qnfs
      Nothing   -> throwAStr $ "Couldn't find qnf for tnnode: " ++ show n

-- | From the value of the node get a query (the reference might not
-- exist in the graph, that's why we return a Maybe)
getNodeQnfN
  :: (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => NodeRef n
  -> m [QNFQuery e s]
getNodeQnfN ref = gets $ fromMaybe [] . (ref `refLU`) . nrefToQnfs
{-# INLINE getNodeQnfN #-}

mkNodeFromQnfT
  :: (Hashables2 e s,Monad m)
  => QNFQuery e s
  -> CGraphBuilderT e s t n m (NodeRef t)
mkNodeFromQnfT qnf = do
  ref <- lift2 newNodeT
  linkTRefQnf ref qnf
  return ref

mkNodeFormQnfNUnsafe
  :: (Hashables2 e s,Monad m)
  => QNFQuery e s
  -> CGraphBuilderT e s t n m (NodeRef n)
mkNodeFormQnfNUnsafe qqnf = do
  ref <- lift2 newNodeN
  -- The new node should have a score
  linkNRefQnf ref qqnf
  return ref

lookupClustersX :: (Traversable f, Hashables2 e s,
                   MonadState (ClusterConfig e s t n) m) =>
                  f (QNFQuery e s)
                -> m (f (QNFQuery e s, [AnyCluster e s t n]))
lookupClustersX = mapM $ \qnf -> gets
  $ (qnf,) . fromMaybe [] . (qnf `HM.lookup`) . qnfToClustMap
{-# INLINE lookupClustersX #-}

-- | Get clusters for which the node is not an input.
getClustersNonInput
  :: forall e s t n m err .
  (Hashables2 e s,MonadState (ClusterConfig e s t n) m,MonadAShowErr e s err m)
  => NodeRef n
  -> m [AnyCluster e s t n]
getClustersNonInput ref =
  filter (notElem ref . clusterInputs) <$> lookupClustersN ref

lookupQnfN
  :: (Hashables2 e s
     ,MonadState (ClusterConfig e s t n) m
     ,MonadAShowErr e s err m)
  => NodeRef n
  -> m [QNFQuery e s]
lookupQnfN ref = gets $ fromMaybe [] . refLU ref . nrefToQnfs

-- | First lookup the QNF and then lookup the cluster.
lookupClustersN
  :: (Hashables2 e s
     ,MonadState (ClusterConfig e s t n) m
     ,MonadAShowErr e s err m)
  => NodeRef n
  -> m [AnyCluster e s t n]
lookupClustersN ref = do
  qnfs <- getNodeQnfN ref
  let q = head qnfs
  gets $ fromMaybe [] . HM.lookup q . qnfToClustMap
-- If all goes well the clusts found should all contain the provided ref
-- let clusts' =
--       filter
--         (\c -> ref
--          `elem` (clusterInputs c ++ clusterOutputs c ++ clusterInterms c))
--         clusts
-- return clusts
{-# INLINE lookupClustersN #-}

lookupClusterT
  :: (Hashables2 e s
     ,MonadState (ClusterConfig e s t n) m
     ,MonadError err m
     ,HasCallStack
     ,AShowError e s err)
  => NodeRef t
  -> m (Maybe (QNFQuery e s,AnyCluster e s t n))
lookupClusterT ref = do
  qnfs <- gets $ refLU ref . trefToQnf
  lookupClustersX qnfs >>= \case
    Nothing -> return Nothing
    Just (qnf,clusts)
      -> case nub $ filter (elem ref . fst . allNodeRefsAnyClust) clusts of
        [clust] -> return $ Just (qnf,clust)
        [] -> return Nothing
        xs -> throwAStr
          $ "Each t node should be in exactly one cluster" ++ ashow (ref,xs)

-- | Multiple join clusters will result in the same node.
lookupClusters :: (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
                 QNFQuery e s
               -> m [AnyCluster e s t n]
lookupClusters qnf = gets $ maybe [] toList . (qnf `HM.lookup`) . qnfToClustMap

-- | From a query get the nodes and the corresponding cluster that
-- express. The total distinct noderefs should be 1 but our QNF
-- mechanism is not perfect so there might be more qnf
-- representations.
matchNode :: forall e s t  n m .
            (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
            QNFQuery e s
          -> m [(NodeRef n, AnyCluster e s t n)]
matchNode qnf = lookupClusters qnf >>= fmap join . mapM go
  where
    go :: AnyCluster e s t n -> m [(NodeRef n, AnyCluster e s t n)]
    go clust = fmap2 (,clust)
      $ filterM (`materializesQnf` qnf)
      $ snd
      $ allNodeRefsAnyClust clust
    materializesQnf :: NodeRef n -> QNFQuery e s -> m Bool
    materializesQnf ref qnf' = dropReader get $ elem qnf' <$> getNodeQnfN ref

linkQnfClust
  :: forall e s t n m .
  (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => QNFQuery e s
  -> AnyCluster e s t n
  -> m ()
linkQnfClust q clust = do
  modify $ \r -> r
    { qnfToClustMap = HM.alter (Just . maybe [clust] (clust :)) q
        $ qnfToClustMap r
    }

registerClusterInput
  :: forall e s t n m .
  (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => NodeRef n
  -> AnyCluster e s t n
  -> m ()
registerClusterInput ref clust = do
  qnfs <- dropReader get $ getNodeQnfN ref
  forM_ qnfs $ \qnf -> linkQnfClust qnf clust

linkNRefQnf
  :: forall e s t n m .
  (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => NodeRef n
  -> QNFQuery e s
  -> m ()
linkNRefQnf ref qnf = do
  refsToQnfs <- gets nrefToQnfs
  -- XXX: If this has worked well for a while remove this check and
  -- enforce the 1-1 corrensondence.
  when (isJust $ refLU ref refsToQnfs) $ error $ "already linked " ++ show ref
  modify $ \r -> r
    { nrefToQnfs = refAlter (Just . maybe [qnf] (qnf :)) ref $ nrefToQnfs r }
clearNRefQnfs
  :: forall e s t n m .
  (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => NodeRef n
  -> m ()
clearNRefQnfs ref = modify $ \r -> r{nrefToQnfs=refDelete ref $ nrefToQnfs r}

linkTRefQnf
  :: forall e s t n m .
  (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => NodeRef t
  -> QNFQuery e s
  -> m ()
linkTRefQnf ref qnf = modify $ \r -> r{trefToQnf=refInsert ref qnf $ trefToQnf r}

clearTRefQnf
  :: forall e s t n m .
  (Hashables2 e s,MonadState (ClusterConfig e s t n) m)
  => NodeRef t
  -> m ()
clearTRefQnf ref = modify $ \r -> r{trefToQnf=refDelete ref $ trefToQnf r}

-- | make or find the nodes
mkNodeFromQnf
  :: (Hashables2 e s,Monad m)
  => QNFQuery e s
  -> CGraphBuilderT e s t n m [NodeRef n]
mkNodeFromQnf qnf = matchNode qnf >>= \case
  [] -> do
    ref <- lift2 newNodeN
    -- The new node should have a score
    linkNRefQnf ref qnf
    return [ref]
  ns -> return $ nub $ fst <$>  ns

isIntermediateClust
  :: (Hashables2 e s
     ,MonadReader (ClusterConfig e s t n) m
     ,MonadAShowErr e s err m)
  => NodeRef n
  -> m Bool
isIntermediateClust ref = do
  clusters <- dropState (ask,const $ return ()) $ lookupClustersN ref
  return $ any (elem ref . clusterInterms) clusters
