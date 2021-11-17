{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
module Data.QueryPlan.Nodes
  (isIntermediate
  ,isProtected
  ,modifyNodeProtection
  ,setNodeStatesToGCState
  ,setNodeStateUnsafe'
  ,setNodeStateUnsafe
  ,isMat
  ,getNodeCost
  ,configLU
  ,totalNodePages
  ,getDataSize
  ,allNodes
  ,nodesInState
  ,nodesInState'
  ,transitionsToMat
  ,getNodeStateReader
  ,getNodeState
  ,unprotect
  ,protect
  ,getNodeProtection
  ,isMaterialized) where

-- import Data.QueryPlan.Frontiers
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bipartite
import qualified Data.List.NonEmpty   as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Query.QuerySize
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.MTL
import           Data.Utils.Tup
import           Text.Printf

isIntermediate :: Monad m => NodeRef n -> PlanT t n m Bool
isIntermediate n = asks (nsMember n . intermediates)
{-# INLINE isIntermediate #-}

isMaterialized :: Monad m => NodeRef n -> PlanT t n m Bool
isMaterialized = fmap isMat . getNodeState
{-# INLINE isMaterialized #-}

isProtected :: Monad m => NodeRef n -> PlanT t n m Bool
isProtected ref = gets (maybe False (> 0) . refLU ref . nodeProtection)

modifyNodeProtection
  :: Monad m => (Count -> Count) -> NodeRef n -> PlanT t n m ()
modifyNodeProtection f r = do
  st <- get
  put $ if r `refMember` nodeProtection st
        then st{nodeProtection=refAdjust f r $ nodeProtection st}
        else st{nodeProtection=refInsert r (f 0) $ nodeProtection st}

getNodeProtection :: Monad m => NodeRef n -> PlanT t n m Count
getNodeProtection n = gets (fromMaybe 0 . refLU n . nodeProtection)

protect :: Monad m => NodeRef n -> PlanT t n m ()
protect = modifyNodeProtection (+1)
unprotect :: Monad m => NodeRef n -> PlanT t n m ()
unprotect n = do
  modifyNodeProtection (\x -> x - 1) n
  st <- get
  let prot = fromMaybe 0 $ refLU n $ nodeProtection st
  if | prot < 0  -> throwError $ NegativeProtection $ fromEnum n
     | prot == 0 -> put st{nodeProtection=n `refDelete` nodeProtection st}
     | True      -> return ()


getNodeState :: Monad m => NodeRef n -> PlanT t n m NodeState
getNodeState = dropReader get . getNodeStateReader
getNodeStateReader :: MonadReader (GCState t n) m => NodeRef n -> m NodeState
getNodeStateReader r = do
  ns <- asks (nodeStates . NEL.head . epochs)
  maybe (return $ Initial NoMat) return $ r `refLU` ns

transitionsToMat :: MonadReader (GCState t n) m => NodeRef n -> m Bool
transitionsToMat = fmap (== Concrete NoMat Mat) . getNodeStateReader

nodesInState'
  :: forall m t n .
  MonadReader (GCState t n) m
  => Bipartite t n
  -> [NodeState]
  -> m [NodeRef n]
nodesInState' graph states =
  if Initial NoMat `elem` states
  then (++) <$> go states <*> unrefNodes else go states
  where
    unrefNodes :: m [NodeRef n]
    unrefNodes = do
      nr <- toNodeList . snd <$> runBuilder graph nodeRefs
      nr' <- asks $ refKeys . nodeStates . NEL.head . epochs
      return $ toNodeList $ fromNodeList nr `nsDifference` fromNodeList nr'
    go :: [NodeState] -> m [NodeRef n]
    go st =
      asks (fmap fst
      . filter ((`elem` st) . snd)
      . refAssocs
      . nodeStates
      . NEL.head
      . epochs)
    runBuilder :: Monad m' => Bipartite t n -> GraphBuilderT t n m' a -> m' a
    runBuilder bp = (`evalStateT` def { gbPropNet = bp })


allNodes :: Monad m => PlanT t n m ([NodeRef t], [NodeRef n])
allNodes = do
  pn <- asks propNet
  fmap (bimap toNodeList toNodeList)
    $ evalStateT nodeRefs
    $ def { gbPropNet = pn }

nodesInState
  :: forall m t n . Monad m => [NodeState] -> PlanT t n m [NodeRef n]
nodesInState st = do
  x <- asks propNet
  dropReader get $ nodesInState' x st

getDataSize :: (HasCallStack,Monad m) => PlanT t n m PageNum
getDataSize = do
  matIni <- fmap2 (Initial Mat,) $ nodesInState [Initial Mat]
  matConcN <- fmap2 (Concrete NoMat Mat,) $ nodesInState [Concrete NoMat Mat]
  matConcM <- fmap2 (Concrete Mat Mat,) $ nodesInState [Concrete Mat Mat]
  budgetM <- asks budget
  matNodes <- mapM (\(st,n) -> (st,n,) <$> totalNodePages n)
    $ matIni ++ matConcN ++ matConcM
  let ret = sum $ trd3 <$> matNodes
  if maybe False (ret >) budgetM then do
    let msg =
          printf
            "Data size exceeds budget (%d / %s)\nMat: %s"
            ret
            (maybe "<unbounded>" show budgetM)
            (ashow matNodes)
    trM $ "ERROR: " ++ msg
    throwPlan msg else return ret

totalNodePages
  :: (MonadError (PlanningError t n) m
     ,MonadReader (GCConfig t n) m
     ,HasCallStack)
  => NodeRef n
  -> m PageNum
totalNodePages ref = do
  let pageSize = 4096
  size <- getDataSizeOf ref
  case pageNum pageSize size of
    Nothing    -> throwError $ RecordLargerThanPageSize pageSize size ref
    Just 0     -> error
      $ printf "Zero page table spotted node %n (size: %s)" ref $ show size
    Just pgNum -> return pgNum
configLU
  :: (MonadReader (GCConfig t n) m
     ,MonadError (PlanningError t n) m
     ,HasCallStack)
  => String
  -> (GCConfig t n -> RefMap n k)
  -> NodeRef n
  -> m k
configLU msg f r = do
  m <- asks f
  case r `refLU` m of
    Nothing -> error
      $ printf "Lookup failed [%s] %n %s" msg r (show $ refKeys m)
    Just i -> return i

getDataSizeOf
  :: (MonadReader (GCConfig t n) m
     ,MonadError (PlanningError t n) m
     ,HasCallStack)
  => NodeRef n
  -> m TableSize
getDataSizeOf = fmap fst . configLU "getDataSizeOf" nodeSizes

-- | Cost of creating a node under the current state of the graph.
getNodeCost :: Monad m => NodeRef n -> PlanT t n m Double
getNodeCost n = do
  -- size <- totalNodePages 4096 n
  st <- getNodeState n
  return $ if isMat st then 1.0 else 0.7

isMat :: NodeState -> Bool
isMat = \case
  Initial m    -> m == Mat
  Concrete _ m -> m == Mat

setNodeStateUnsafe
  :: forall t n m .
  (HasCallStack,Monad m)
  => NodeRef n
  -> NodeState
  -> PlanT t n m ()
setNodeStateUnsafe = setNodeStateUnsafe' True
setNodeStateUnsafe'
  :: forall t n m .
  (HasCallStack,Monad m)
  => Bool
  -> NodeRef n
  -> NodeState
  -> PlanT t n m ()
setNodeStateUnsafe' verbose r s = do
  gcst <- get
  oldState <- getNodeState r
  let epoch = NEL.head $ epochs gcst
  let epoch' = epoch { nodeStates = r `refInsert` s $ nodeStates epoch }
  put $ gcst { epochs = epoch' NEL.:| NEL.tail (epochs gcst) }
  totalSize <- getDataSize
  when verbose $ do
    trM
      $ printf
        "Unsafe shift %s: %s -> %s [size: %d]"
        (show r)
        (show oldState)
        (show s)
        totalSize


-- | Set a list of nodes to (Initial Mat)
setNodeStatesToGCState :: forall t n .
                         [NodeRef n]
                       -> GCConfig t n
                       -> GCState t n
                       -> GCState t n
setNodeStatesToGCState nrefs _gcc st@GCState {..} =
  st { epochs = headEpoch { nodeStates = newNs
                          }
         NEL.:| restEpochs
     }
  where
    (headEpoch NEL.:| restEpochs) = epochs
    newNs = foldl f (nodeStates headEpoch) nrefs
      where
        f nodeStateMap n = case n `refLU` nodeStateMap of
          Nothing -> refInsert n (Initial Mat) nodeStateMap
          _       -> nodeStateMap
