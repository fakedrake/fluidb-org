{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Data.Cluster.ClusterConfig
  ( getClustersNonInput
  , getNodeCnfN
  , registerClusterInput
  , mkNodeFromCnfT
  , mkNodeFromCnfN
  , lookupClustersX
  , lookupClustersN
  , lookupClusterT
  , replaceCluster
  , lookupClusters
  , matchNode
  , linkCnfClust
  , linkNRefCnfs
  , clearNRefCnfs
  , linkTRefCnf
  , clearTRefCnf
  -- , putEmptyCNFQ
  -- , putIdentityCNFQ
  , mkNodeFromCnf
  , mkNodeFormCnfNUnsafe
  , isIntermediateClust
  ) where

import Data.CnfQuery.Types
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.BipartiteGraph
import           Data.Cluster.Types
import qualified Data.HashMap.Strict  as HM
import           Data.List
import           Data.Maybe
import           Data.NodeContainers
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL

replaceCluster
  :: (MonadError err m
     ,AShowError e s err
     ,Hashables2 e s
     ,MonadState (ClusterConfig e s t n) m)
  => AnyCluster e s t n
  -> AnyCluster e s t n
  -> m ()
replaceCluster cold cnew = do
  cnfsT <- mapM luCNFT $ fst $ allNodeRefsAnyClust cold
  cnfsN <- fmap join $ mapM luCNFN $ snd $ allNodeRefsAnyClust cold
  -- XXX We are finding cnfs but not props!!
  props <- (HM.lookup cold . cnfPropagators) <$> get >>= \case
      Nothing -> throwAStr $ "Not in update map: " ++ ashow (cold,cnew,cnfsN )
      Just x -> return x
  modify $ \cc -> cc{
    cnfPropagators=HM.insert cnew props $ HM.delete cold $ cnfPropagators cc}
  forM_ (nub $ cnfsN ++ cnfsT) $ \cnf ->
    modify $ \cc -> cc{
      cnfToClustMap=HM.adjust ((cnew:) . filter (/= cold)) cnf
        $ cnfToClustMap cc}
  where
    luCNFT t = refLU t . trefToCnf <$> get >>= \case
      Just cnf -> return cnf
      Nothing -> throwAStr $ "Couldn't find cnf for t node: " ++ show t
    luCNFN n = refLU n . nrefToCnfs <$> get >>= \case
      Just cnfs -> return cnfs
      Nothing -> throwAStr $ "Couldn't find cnf for tnnode: " ++ show n

-- | From the value of the node get a query (the reference might not
-- exist in the graph, that's why we return a Maybe)
getNodeCnfN :: (Hashables2 e s, MonadReader (ClusterConfig e s t n) m) =>
               NodeRef n -> m [CNFQuery e s]
getNodeCnfN ref = dropState (ask,const $ return ())
  $ fromMaybe [] . (ref `refLU`) . nrefToCnfs <$> ask
{-# INLINE getNodeCnfN #-}

mkNodeFromCnfT :: (Hashables2 e s, Monad m) =>
                 CNFQuery e s
               -> CGraphBuilderT e s t n m (NodeRef t)
mkNodeFromCnfT cnf = do
  (_isNew, ref) <- lift2 $ newNodeL Nothing
  linkTRefCnf ref cnf
  return ref

mkNodeFormCnfNUnsafe :: (Hashables2 e s, Monad m) =>
                       CNFQuery e s
                     -> CGraphBuilderT e s t n m (NodeRef n)
mkNodeFormCnfNUnsafe qcnf = do
  (_isNew, ref) <- lift2 $ newNodeR Nothing
  -- The new node should have a score
  linkNRefCnfs ref [qcnf]
  return ref

-- Make or find nodes
mkNodeFromCnfN :: (Hashables2 e s, Monad m) =>
                 CNFQuery e s
               -> CGraphBuilderT e s t n m
               (Either (NodeRef n) [(NodeRef n, AnyCluster e s t n)])
mkNodeFromCnfN cnf = matchNode cnf >>= \case
  [] -> Left <$> mkNodeFormCnfNUnsafe cnf
  ns -> return $ Right ns

lookupClustersX :: (Traversable f, Hashables2 e s,
                   MonadState (ClusterConfig e s t n) m) =>
                  f (CNFQuery e s)
                -> m (f (CNFQuery e s, [AnyCluster e s t n]))
lookupClustersX = mapM $ \cnf ->
  (cnf,) . fromMaybe [] . (cnf `HM.lookup`) . cnfToClustMap <$> get
{-# INLINE lookupClustersX #-}

getClustersNonInput :: forall e s t n m .
                      (Hashables2 e s, MonadReader (ClusterConfig e s t n) m) =>
                      NodeRef n
                    -> m [(CNFQuery e s, [AnyCluster e s t n])]
getClustersNonInput ref = fmap (filter $ not . null . snd)
  $ fmap3 (filter $ notElem ref . clusterInputs)
  $ dropState (ask,const $ return ())
  $ lookupClustersN ref

lookupClustersN :: (Hashables2 e s,
                   MonadState (ClusterConfig e s t n) m) =>
                  NodeRef n
                -> m [(CNFQuery e s, [AnyCluster e s t n])]
lookupClustersN ref = lookupClustersX =<< dropReader get (getNodeCnfN ref)
{-# INLINE lookupClustersN #-}

lookupClusterT :: (Hashables2 e s,
                  MonadState (ClusterConfig e s t n) m,
                  MonadError err m, HasCallStack,
                  AShowError e s err) =>
                  NodeRef t
                -> m (Maybe (CNFQuery e s, AnyCluster e s t n))
lookupClusterT ref =
  (refLU ref . trefToCnf <$> get >>= lookupClustersX) >>=
  \case
    Nothing -> return Nothing
    Just (cnf, clusts) ->
      case nub $ filter (elem ref . fst . allNodeRefsAnyClust) clusts of
        [clust] -> return $ Just (cnf, clust)
        []      -> return Nothing
        xs      -> throwAStr
          $ "Each t node should be in exactly one cluster" ++ ashow (ref,xs)

-- | Multiple join clusters will result in the same node.
lookupClusters :: (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
                 CNFQuery e s
               -> m [AnyCluster e s t n]
lookupClusters cnf = maybe [] toList . (cnf `HM.lookup`) . cnfToClustMap <$> get

-- | From a query get the nodes and the corresponding cluster that
-- express. The total distinct noderefs should be 1 but our CNF
-- mechanism is not perfect so there might be more cnf
-- representations.
matchNode :: forall e s t  n m .
            (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
            CNFQuery e s
          -> m [(NodeRef n, AnyCluster e s t n)]
matchNode cnf = lookupClusters cnf >>= fmap join . mapM go
  where
    go :: AnyCluster e s t n -> m [(NodeRef n, AnyCluster e s t n)]
    go clust = fmap2 (,clust)
      $ filterM (`materializesCnf` cnf)
      $ snd
      $ allNodeRefsAnyClust clust
    materializesCnf :: NodeRef n -> CNFQuery e s -> m Bool
    materializesCnf ref cnf' = dropReader get $ elem cnf' <$> getNodeCnfN ref

linkCnfClust :: forall e s t n m .
               (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
               CNFQuery e s
             -> AnyCluster e s t n
             -> m ()
linkCnfClust q clust =
  modify $ \r -> r{cnfToClustMap =
                  HM.alter (Just . maybe [clust] (clust:)) q $ cnfToClustMap r}

registerClusterInput :: forall e s t n m .
                        (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
                        NodeRef n
                      -> AnyCluster e s t n
                      -> m ()
registerClusterInput ref clust = do
  cnfs <- dropReader get $ getNodeCnfN ref
  forM_ cnfs $ \cnf -> linkCnfClust cnf clust

linkNRefCnfs :: forall e s t n m . (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
               NodeRef n
             -> [CNFQuery e s]
             -> m ()
linkNRefCnfs ref cnfs =
  modify $ \r -> r{nrefToCnfs=refAlter (Just . maybe cnfs (cnfs ++)) ref
                             $ nrefToCnfs r}
clearNRefCnfs :: forall e s t n m . (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
               NodeRef n
             -> m ()
clearNRefCnfs ref = modify $ \r -> r{nrefToCnfs=refDelete ref $ nrefToCnfs r}

linkTRefCnf :: forall e s t n m . (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
              NodeRef t
            -> CNFQuery e s
            -> m ()
linkTRefCnf ref cnf = modify $ \r -> r{trefToCnf=refInsert ref cnf $ trefToCnf r}

clearTRefCnf :: forall e s t n m . (Hashables2 e s, MonadState (ClusterConfig e s t n) m) =>
               NodeRef t
             -> m ()
clearTRefCnf ref = modify $ \r -> r{trefToCnf=refDelete ref $ trefToCnf r}

-- | make or find the nodes
mkNodeFromCnf :: (Hashables2 e s, Monad m) =>
                CNFQuery e s -> CGraphBuilderT e s t n m [NodeRef n]
mkNodeFromCnf = fmap (either return $ nub . fmap fst) . mkNodeFromCnfN

isIntermediateClust :: (Hashables2 e s, MonadReader (ClusterConfig e s t n) m) =>
                      NodeRef n -> m Bool
isIntermediateClust ref = do
  clusters <- fmap (>>= snd) $ dropState (ask,const $ return ())
    $ lookupClustersN ref
  return $ any (elem ref . clusterInterms) clusters
