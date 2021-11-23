{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# OPTIONS_GHC -Wno-deprecations #-}

module Data.QueryPlan.Solve
  (setNodeMaterialized) where

import           Control.Antisthenis.Types
import           Control.Monad.Cont
import           Control.Monad.Extra
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer            hiding (Sum)
import qualified Data.HashSet                    as HS
import           Data.List.Extra
import qualified Data.List.NonEmpty              as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Query.QuerySize
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.History
import           Data.QueryPlan.MetaOpCache
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Transitions
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.ListT
import           Data.Utils.Tup

import           Data.Proxy
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.GC
import           Data.QueryPlan.HistBnd
import           Data.QueryPlan.Matable          as Mat
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Types
import           Data.QueryPlan.Utils
import           Data.Utils.AShow.Print
import           Data.Utils.HCntT
import           Data.Utils.Nat



setNodeMaterialized
  :: forall t n m . MonadLogic m => NodeRef n -> PlanT t n m ()
setNodeMaterialized node = wrapTrace ("setNodeMaterialized " ++ show node) $ do
  -- sizes <- asks nodeSizes
  -- Populate the metaop cache
  -- warmupCache node
  reportBudget
  isMat <- Mat.isMaterializable [] node
  when (not isMat) $ error $ "Not materializable node: " ++ show node
  setNodeStateSafe node Mat
  -- curateTransitions
  cost <- totalTransitionCost
  size <- getDataSize
  traceM
    $ printf
      "Successfully materialized %s -- cost: %s, totalsize : %s"
      (show node)
      (show cost)
      (show size)
  reportBudget

reportBudget :: Monad m => PlanT t n m ()
reportBudget = do
  mats <- nodesInState [Initial Mat,Concrete NoMat Mat,Concrete Mat Mat]
  mats' <- forM mats  $ \n -> (n,) <$> totalNodePages n
  trM $ "mat nodes: " ++ show mats'
  size <- getDataSize
  trM $ "Used budget: " ++ show size


haltPlan
  :: (HaltKey m ~ PlanSearchScore,MonadHalt m)
  => NodeRef n
  -> MetaOp t n
  -> PlanT t n m ()
haltPlan matRef mop = do
  -- From the frontier replace matRef with it's dependencies.
  modify $ \gcs -> gcs
    { frontier = nsDelete matRef (frontier gcs) <> metaOpIn mop }
  extraCost <- metaOpCost [matRef] mop
  void $ haltPlanCost extraCost

histCosts :: Monad m => Cost -> PlanT t n m Cost
histCosts maxCost = do
  hcs :: [Maybe (HistVal Cost)] <- takeListT 3 $ pastCosts maxCost
  trM $ printf "Historical costs: %s" $ ashowLine $ ashow hcs
  -- Curate the consts that are too likely to be non-comp
  return $ mconcat $ mapMaybe (>>= toCost) hcs
  where
    toCost HistVal {..} = if hvNonComp > 0.6 then Nothing else Just hvVal

-- | Compute the frontier cost and the historical costs.
haltPlanCost
  :: forall t n m .
  (HaltKey m ~ PlanSearchScore,MonadHalt m,HasCallStack)
  => Cost
  -> PlanT t n m Cost
haltPlanCost concreteCost = wrapTrM "haltPlanCost" $ do
  frefs <- gets $ toNodeList . frontier
  costs <- forM frefs $ \ref -> do
    cost <- getPlanBndR @(CostParams CostTag n) Proxy mempty ForceResult ref
    case cost of
      BndRes (Sum (Just r)) -> return $ pcCost r
      BndRes (Sum Nothing) -> throwPlan $ "Infinite cost: " ++ ashow ref
      BndBnd _bnd -> throwPlan "Got bound instead of cost"
      BndErr e -> do
        matSt <- getNodeState ref
        throwPlan
          $ "getPlanBndR("
          ++ ashow (ref,matSt)
          ++ "):antisthenis error: "
          ++ ashow e
  let frontierCost :: Double = sum [fromIntegral $ costAsInt c | c <- costs]
  hc <- histCosts $ concreteCost <> mconcat costs
  trM $ printf "Halt%s: %s" (show frefs) $ show concreteCost
  halt
    $ PlanSearchScore
      (fromIntegral $ costAsInt concreteCost)
      (Just $ frontierCost + fromIntegral (costAsInt hc))
  trM "Resume!"
  return hc

setNodeStateSafe
  :: (HaltKey m ~ PlanSearchScore,MonadLogic m)
  => NodeRef n
  -> IsMat
  -> PlanT t n m ()
setNodeStateSafe n = setNodeStateSafe' (findPrioritizedMetaOp lsplit n) n
{-# INLINE setNodeStateSafe' #-}

setNodeStateSafe'
  :: MonadLogic m
  => PlanT t n m (MetaOp t n)
  -> NodeRef n
  -> IsMat
  -> PlanT t n m ()
setNodeStateSafe' getFwdOp node goalState =
  wrapTrM (printf "setNodeStateSafe %s %s" (show node) (show goalState)) $ do
    curState <- getNodeState node
    trM
      $ printf
        "Safe shift %s: %s -> %s"
        (show node)
        (show curState)
        (show goalState)
    case curState of
      Concrete _ Mat -> case goalState of
        Mat   -> top
        NoMat -> bot "Tried to set concrete"
      Concrete _ NoMat -> case goalState of
        NoMat -> top
        Mat   -> bot "Tried to set concrete" -- XXX: Check that it is not in the frontier
      Initial NoMat -> case goalState of
        NoMat -> node `setNodeStateUnsafe` Concrete NoMat NoMat
        Mat       -- just materialize
          -> do
            node `setNodeStateUnsafe` Concrete NoMat NoMat
            forwardMop <- getFwdOp
            let interm = toNodeList $ metaOpInterm forwardMop
            let depset = toNodeList $ metaOpIn forwardMop
            let trigAction = metaOpPlan forwardMop >>= mapM_ putTransition
            guardlM
              ("Nodes don't fit in budget" ++ show (node : depset ++ interm))
              $ nodesFit
              $ node : depset ++ interm
            haltPlan node forwardMop
            withProtected (node : depset ++ interm) $ do
              trM $ "Materializing dependencies: " ++ show depset
              setNodesMatSafe depset
              trM $ "Intermediates: " ++ show interm
              once $ garbageCollectFor $ node : interm
              -- Automatically set the states of intermediates
              forM_ interm $ \ni -> do
                prevState' <- getNodeState ni
                let prevState = case prevState' of
                      Concrete _ r -> r
                      Initial r    -> r
                ni `setNodeStateUnsafe` Concrete prevState Mat
              -- Deal with sibling materializability: what we actually
              -- want is to be able to reverse.
              splitOnOutMaterialization node forwardMop
              trigAction
      Initial Mat -> case goalState of
        Mat -> node `setNodeStateUnsafe` Concrete Mat Mat
        NoMat -> do
          node `setNodeStateUnsafe` Concrete Mat NoMat
          Mat.isMaterializableSlow [] node
            >>= guardl ("not materializable, can't delete " ++ ashow node)
          delDepMatCache node
          putDelNode node

-- | Set a list of nodes to Mat state in an order that is likely to
-- require the least budget. Just find a random metaop for each and
-- sort according to that.
setNodesMatSafe
  :: (HaltKey m ~ PlanSearchScore,MonadLogic m)
  => [NodeRef n]
  -> PlanT t n m ()
setNodesMatSafe deps = do
  hbM <- getHardBudget
  (matDeps,unMatDeps) <- partitionM isMaterialized deps
  forM_ deps $ \n -> setNodeStateSafe n Mat
  ret <- fmap (sortOn (\(_,x,_) -> x)) $ forM unMatDeps $ \dep -> do
    pgs <- totalNodePages dep
    -- Unless it's already materialized mat.
    trigMops <- findTriggerableMetaOps dep
    fp <- maybe (bot $ printf "No metaops for %n" dep) metaOpNeededPages
      $ listToMaybe trigMops
    return (pgs,fp,dep)
  case hbM of
    Nothing -> top
    Just hb -> guardl ("no valid trigger sequence for deps: " ++ show ret)
      $ all (<= hb)
      $ zipWith (\(_,x,_) y -> x + y) ret
      $ scanl (+) 0
      $ fst3 <$> ret
  mapM_ (`setNodeStateSafe` Mat) $ matDeps ++ fmap trd3 ret

-- | Split on each different style of materialization.
splitOnOutMaterialization
  :: MonadLogic m => NodeRef n -> MetaOp t n -> PlanT t n m ()
splitOnOutMaterialization ref forwardMop = do
  ref `setNodeStateUnsafe` Concrete NoMat Mat
  rMops <- reverseMops forwardMop
  trM $ "Reverse mops of: " ++ showMetaOp forwardMop
  forM_ rMops $ \rMop -> trM $ "\t" ++ showMetaOp rMop
  rMopsNotNoop <- filterM (fmap not . isTriggerable) rMops
  case rMopsNotNoop of
    [] -> top
    -- First try solving the empty case. If that works Note that top
    -- is similar to the case where we materialize the complementary
    -- nodes and then garbage collect them but note that they will be
    -- deleted only after a new epoch.
    _  -> mapM_ makeTriggerableUnsafe rMopsNotNoop `eitherl` top

isTriggerable :: MonadLogic m => MetaOp t n -> PlanT t n m Bool
isTriggerable mop = all isMat <$> mapM getNodeState (toNodeList $ metaOpIn mop)

-- | If any of the inputs `Concrete _ NoMat` fail (not triggerable)
-- Protect all the inputs, intermediates and outputs
--   Garbage collect so that the non-materialized inputs fit.
--
-- The assumption here is tha the inputs of this are already
-- materialized due to a pending trigger.
makeTriggerableUnsafe :: MonadLogic m => MetaOp t n -> PlanT t n m ()
makeTriggerableUnsafe mop = wrapTrM ("makeTriggerableUnsafe " ++ showMetaOp mop) $ do
  let outList = toNodeList $ metaOpOut mop
  let inList = toNodeList $ metaOpIn mop
  let intermList = toNodeList $ metaOpInterm mop
  inStates <- mapM getNodeState $ inList ++ intermList
  -- If we are committed to not materializing some of the nodes honor
  -- that commitment, we know the rest will fail
  guardl "Concrete _ NoMat in trig deps"
    $ all (\case{Concrete _ NoMat -> False; _ -> True}) inStates
  withProtected (inList ++ outList ++ intermList) $ do
    nonMatInList <- filterM (fmap (not . isMat) . getNodeState) inList
    -- The materialized nodes do not exceed our space
    garbageCollectFor nonMatInList
    -- All input s are concretely materialized
    mapM_ setConcreteMat inList

setConcreteMat :: MonadLogic m => NodeRef n -> PlanT t n m ()
setConcreteMat ref = getNodeState ref >>= \case
  Concrete _ NoMat -> bot "setConcreteMat"
  Concrete _ Mat   -> top
  Initial m        -> setNodeStateUnsafe ref $ Concrete m Mat

-- |Remove equivalent metaops from list
nubMops :: [MetaOp t n] -> [MetaOp t n]
nubMops = nubOn $ \mop -> (metaOpIn mop, metaOpOut mop)

-- | Find MetaOps that from some of metaOpOut will produce some of the
-- metaOpIn.
reverseMops :: forall t n m . Monad m => MetaOp t n -> PlanT t n m [MetaOp t n]
reverseMops fmop = nubMops . filter validate . join
  <$> mapM findMetaOps (toNodeList $ metaOpIn fmop)
  where
    validate :: MetaOp t n -> Bool
    validate rmop = (metaOpIn rmop `nsIsSubsetOf` metaOpOut fmop)
                    && (metaOpOut rmop `nsIsSubsetOf` metaOpIn fmop)

withProtected :: MonadLogic m => [NodeRef n] -> PlanT t n m a -> PlanT t n m a
withProtected [] m = m
withProtected (n:ns) m = do
  prot <- forM (n:ns) $ \nref -> (nref,) <$> getNodeProtection nref
  wrapTrM ("withProtected " ++ show prot) $ do
    forM_ (n:ns) protect
    ret <- m
    forM_ (n:ns) unprotect
    return ret

newEpoch :: MonadLogic m => PlanT t n m ()
newEpoch = wrapTrM "newEpoch" $ do
  st <- get
  let protected = nodeProtection st
  let epoch@GCEpoch {..} NEL.:| rest = epochs st
  if (epoch,protected)
    `HS.member` epochFilter st then bot "Epoch already encountered"
    else let epochFilter' = HS.insert (epoch,protected) $ epochFilter st
             -- demote all except the protected
             maybeDemote r x = case x of
               Concrete _ mat -> if fromMaybe 0 (r `refLU` protected) > 0
                 then return x else tell [(r,mat)] >> return (Initial mat)
               _ -> return x
             (newStates,updatedRefs) =
               runWriter $ refTraverseWithKey maybeDemote nodeStates
      in if newStates == nodeStates then bot "No change in node states." else do
        put
          $ st { epochs = GCEpoch { nodeStates = newStates,transitions = [] }
                   NEL.:| (epoch : rest)
                ,epochFilter = epochFilter'
               }
        trM $ "New epoch. Demoted refs: " ++ show updatedRefs

garbageCollectFor
  :: forall t n m . (MonadLogic m) => [NodeRef n] -> PlanT t n m ()
garbageCollectFor
  ns = wrapTrM ("garbageCollectFor " ++ show ns) $ withGC $ \requiredPages -> do
  preReport
  hc <- haltPlanCost zero
  go requiredPages hc `eitherl` (newEpoch >> go requiredPages hc)
  trM $ "Finished GC to make " ++ show ns
  where
    preReport = do
      nsize <- sum <$> traverse totalNodePages ns
      totalSize <- getDataSize
      budget <- asks $ maybe "<unboundend>" show . budget
      nsmap <- forM ns $ \n -> (n,) <$> totalNodePages n
      trM
        $ printf
          "Starting GC to make (%d / %s) %s: %d"
          totalSize
          budget
          (show nsmap)
          nsize
    withGC m = do
      isGC <- gets garbageCollecting
      if isGC then bot "nested GCs" else do
        setGC True
        -- Check that gc is possiblex
        requiredPages <- sum <$> traverse totalNodePages ns
        whenM (isOversized requiredPages) $ m requiredPages
        setGC False
      where
        setGC b = modify $ \x -> x { garbageCollecting = b }
    go :: PageNum -> Cost -> PlanT t n m ()
    go requiredPages hc = do
      assertGcIsPossible requiredPages
      savedPagesInterm :: PageNum <- killIntermediates
      let whenOversized pgs m = if pgs >= 0 then m else return (0 :: PageNum)
      savedPagesPrim
        :: PageNum <- whenOversized (requiredPages - savedPagesInterm)
        $ killPrimaries (requiredPages - savedPagesInterm) hc
      trM
        $ "New goal for pages: "
        ++ show (requiredPages - savedPagesInterm - savedPagesPrim)
      void
        $ whenOversized (requiredPages - savedPagesInterm - savedPagesPrim)
        $ bot
        $ "GC failed: " ++ show ns

nodesFit :: Monad m => [NodeRef n] -> PlanT t n m Bool
nodesFit ns = do
  protec <- fmap sum . traverse totalNodePages
           =<< filterM (fmap (> 0) . getNodeProtection)
           =<< nodesInState [Concrete NoMat Mat,Concrete Mat Mat]
  size <- sum <$> traverse totalNodePages ns
  budg <- asks budget
  trM $ printf "Needed node size: %d, budget: %s, protec: %d"
    size (show budg) protec
  return $ maybe True (size + protec <=) budg

-- |A very quick test to filter out blatantly impossible gc
-- attempts. Checks if concrete nodes are respected
assertGcIsPossible :: MonadPlus m => PageNum -> PlanT t n m ()
assertGcIsPossible requiredPages = do
  budg <- asks budget
  let sizeOfSt sts = fmap sum . traverse totalNodePages =<< nodesInState sts
  concr <- sizeOfSt [Concrete Mat Mat,Concrete NoMat Mat]
  ini <- sizeOfSt [Initial Mat]
  when (maybe False (requiredPages + concr >) budg)
    $ bot
    $ printf
      "assertGcIsPossible : needed=%d, concrete=%d, budget=%s, ini=%d"
      requiredPages
      concr
      (show budg)
      ini

killIntermediates :: forall t n m . MonadLogic m => PlanT t n m PageNum
killIntermediates = do
  intermediates <- asks $ toNodeList . intermediates
  matInterm <- filterM isMaterialized intermediates
  nonProtectedMatInterm <- filterM (fmap not . isProtected) matInterm
  pgs <- forM nonProtectedMatInterm $ \x -> do
    delDepMatCache x
    x `setNodeStateUnsafe` Concrete Mat NoMat
    putDelNode x
    totalNodePages x
  return $ sum pgs

isOversized
  :: forall t n m . MonadLogic m => PageNum -> PlanT t n m Bool
isOversized requiredPages = do
  totalSize <- getDataSize
  budget <- asks budget
  if maybe False (\b -> requiredPages + totalSize > b) budget then do
    trM
      $ printf
        "oversized: %d / %s (needed: %d)"
        totalSize
        (maybe "<unbounded>" show budget)
        requiredPages
    return True else return False

killPrimaries :: MonadLogic m => PageNum -> Cost -> PlanT t n m PageNum
killPrimaries requiredPages hc = wrapTrM "killPrimaries" $ do
  nsUnordMaybeIsolated <- nodesInState [Initial Mat]
  nsUnord <- filterM isDeletable nsUnordMaybeIsolated
  nsSized <- forM nsUnord $ \ref -> (,ref) <$> totalNodePages ref
  guardl "Not enough deletable nodes"
    $ sum (fst <$> nsSized) > requiredPages
  ds <- delSets nsUnord
  safeDelInOrder requiredPages ds


-- | Try to delete the node groups in the order they are presented
-- until the required pages are freed.
safeDelInOrder
  :: forall t n m .
  MonadLogic m
  => PageNum
  -> [[NodeRef n]]
  -> PlanT t n m PageNum
safeDelInOrder requiredPages nsOrd =
  wrapTrM ("safeDelInOrder " ++ show nsOrd) $ delRefs 0 [] nsOrd
  where
    delRefs :: PageNum -> [[NodeRef n]] -> [[NodeRef n]] -> PlanT t n m PageNum
    delRefs freed _prev [] = return freed
    delRefs freed prev (refs:rest) = do
      extraFree <- delOrConcreteMat refs
      if freed + extraFree > requiredPages then return $ freed + extraFree
        else delRefs (freed + extraFree) (refs : prev) rest
    delOrConcreteMat refs = do
      canStillDel <- allM isDeletable refs
      trM $ "Considering deletion: " ++ show (refs,canStillDel)
      if canStillDel then sum <$> mapM toDelMaybe refs else return 0
    toDelMaybe :: NodeRef n -> PlanT t n m PageNum
    toDelMaybe ref = do
      wasMat <- isMat <$> getNodeState ref
      ref `setNodeStateSafe` NoMat
      if wasMat then totalNodePages ref else return 0

isDeletable :: MonadLogic m => NodeRef n -> PlanT t n m Bool
isDeletable ref = do
  trM $ "[Before] isDeletable" <: ref
  isd <- getNodeState ref >>= \case
    Concrete _ Mat -> return False
    _x             -> Mat.isMaterializableSlow [ref] ref
  trM $ "[After] isDeletable" <: (ref,isd)
  return isd

-- Nodes
