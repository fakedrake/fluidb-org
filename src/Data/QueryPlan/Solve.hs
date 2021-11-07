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
{-# LANGUAGE ViewPatterns          #-}
{-# OPTIONS_GHC -Wno-deprecations #-}

module Data.QueryPlan.Solve
  (setNodeMaterialized) where

import           Control.Antisthenis.Types
import           Control.Monad.Cont
import           Control.Monad.Except
import           Control.Monad.Extra
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer        hiding (Sum)
import           Data.Bipartite
import qualified Data.HashSet                as HS
import           Data.List.Extra
import qualified Data.List.NonEmpty          as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Query.QuerySize
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.Dependencies
import           Data.QueryPlan.History
import           Data.QueryPlan.MetaOpCache
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Transitions
import           Data.Utils.Debug
import           Data.Utils.Functors
-- import           Data.Utils.HContT
import           Data.Utils.ListT
import           Data.Utils.Tup

import           Data.Proxy
import           Data.QueryPlan.Cert
import           Data.QueryPlan.Comp
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Types
import           Data.QueryPlan.Utils
import           Data.Utils.AShow.Print
import           Data.Utils.Default
import           Data.Utils.HCntT
import           Data.Utils.Nat


#ifdef DEPRECATED
-- | run a bunch of common stuff to make the following more responsive.
warmupCache :: forall t n m . Monad m => NodeRef n -> PlanT t n m ()
warmupCache node = do
  trM "Warming up cache..."
  gcs <- get
  gcc <- ask
  case runIdentity $ runExceptT $ (`runReaderT` gcc) $ (`execStateT` gcs) doWarmup of
    Left e     -> throwError e
    Right gcs' -> trM "Cache warm!" >> put gcs'
  where
    doWarmup :: PlanT t n Identity ()
    doWarmup = wrapTrace ("doWarmup" ++ show node) $ do
      xs <- runListT $ getDependencies node
      when (null xs) $ throwPlan $ printf "No deps found for %n" node
      when (any (nsNull . fst) xs) $ throwPlan
        $ printf "Empty depset for %n: %s" node (ashow xs)
      trM $ printf "Deps of %n: %s" node $ ashow xs
      mapM_ findMetaOps =<< asks (refKeys . nodeSizes)
#endif

setNodeMaterialized
  :: forall t n m . MonadLogic m => NodeRef n -> PlanT t n m ()
setNodeMaterialized node = wrapTrace ("setNodeMaterialized " ++ show node) $ do
  -- sizes <- asks nodeSizes
  -- Populate the metaop cache
  -- warmupCache node
  setNodeStateSafe node Mat
  -- curateTransitions
  cost <- totalTransitionCost
  trM
    $ printf "Successfully materialized %s -- cost: %s" (show node) (show cost)

-- | Concretify materializability
makeMaterializable
  :: forall t n m . (HasCallStack,MonadLogic m) => NodeRef n -> PlanT t n m ()
makeMaterializable ref =
  wrapTrM ("makeMaterializable " ++ show ref) $ checkCache $ withNoMat ref $ do
    (deps,_star) <- foldrListT1
      (eitherl . return)
      (bot $ "no dependencies for " ++ show ref)
      $ getDependencies ref
    trM $ printf "Deps of %s: %s" (show ref) (show $ toNodeList deps)
    forM_ (toNodeList deps) $ \dep -> getNodeState dep >>= \case
      Initial Mat -> setNodeStateUnsafe dep $ Concrete Mat Mat
      Concrete _ Mat -> top
      s -> throwPlan $ "getDependencies returned node in state: " ++ show s
  where
    checkCache m = luMatCache ref >>= \case
      Nothing -> m
      Just (frontierStar -> (deps,_star)) ->
        unlessM (allM isConcreteMat $ toNodeList deps) m
    isConcreteMat =
      fmap (\case
              Concrete _ Mat -> True
              _              -> False) . getNodeState

haltPlan :: (HaltKey m ~ PlanSearchScore,MonadHalt m) => NodeRef n -> MetaOp t n -> PlanT t n m ()
haltPlan matRef mop = do
  -- From the frontier replace matRef with it's dependencies.
  modify $ \gcs -> gcs
    { frontier = nsDelete matRef (frontier gcs) <> metaOpIn mop }
  extraCost <- metaOpCost [matRef] mop
  void $  haltPlanCost Nothing $ fromIntegral $ costAsInt extraCost

histCosts :: Monad m => PlanT t n m Cost
histCosts = do
  hcs :: [Maybe HCost] <- takeListT 5 $ pastCosts mempty
  -- Curate the consts that are too likely to be non-comp
  return $ mconcat $ mapMaybe (>>= toCost) hcs
  where
    toCost (Cert _ (Comp c v)) = if c > 0.6 then Nothing else Just v


-- | Compute the frontier cost and the historical costs.
haltPlanCost
  :: (HaltKey m ~ PlanSearchScore,MonadHalt m,HasCallStack)
  => Maybe Cost
  -> Double
  -> PlanT t n m Cost
haltPlanCost histCostCached concreteCost = do
  frefs <- gets $ toNodeList . frontier
  (costs,_extraNodes) <- runWriterT $ forM frefs $ \ref -> do
    cost <- lift $ getCostPlan @CostTag Proxy mempty ForceResult ref
    case cost of
      Nothing -> return zero
      Just c -> do
        maybe (return ()) tell $ pcPlan c
        return $ pcCost c
  let frontierCost :: Double = sum [fromIntegral $ costAsInt c | c <- costs]
  hc <- maybe histCosts return histCostCached
  trM $ printf "Halt%s: %s" (show frefs) $ show concreteCost
  trM $ printf "Historical costs: %s" $ ashowLine $ ashow hc
  halt
    $ PlanSearchScore
      concreteCost
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
            -- xxx: setNodeMatHistoricalCosts node
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
          isMaterializable node
            >>= guardl ("not materializable, can't delete " ++ ashow node)
          delDepMatCache node
          putDelNode node

-- | Set a list of nodes to Mat state in an order that is likely to
-- require the least budget.
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
    fp <- maybe (bot $ printf "No metaops for %n" dep) metaOpNeededPages
      . listToMaybe
      =<< findTriggerableMetaOps dep
    return (pgs,fp,dep)
  case hbM of
    Nothing -> top
    Just hb -> guardl ("no valid trigger sequence for deps: " ++ show ret)
      $ all (<= hb)
      $ zipWith (\(_,x,_) y -> x + y) ret
      $ scanl (+) 0
      $ fst3 <$> ret
  mapM_ (`setNodeStateSafe` Mat) $ fmap trd3 ret ++ matDeps

-- | Split on each different style of materialization.
splitOnOutMaterialization :: MonadLogic m =>
                            NodeRef n -> MetaOp t n -> PlanT t n m ()
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
    _  -> mapM_ makeTriggerableUnsafe rMopsNotNoop `lsplit` top

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
  hc <-  return mempty -- haltPlanCost Nothing 0
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
  let nsOrd = snd <$> sortOn fst nsSized
  safeDelInOrder requiredPages hc nsOrd

safeDelInOrder
  :: forall t n m .
  MonadLogic m
  => PageNum
  -> Cost
  -> [NodeRef n]
  -> PlanT t n m PageNum
safeDelInOrder requiredPages hc nsOrd =
  wrapTrM ("safeDelInOrder " ++ show nsOrd) $ delRefs 0 [] nsOrd
  where
    delRefs :: PageNum -> [NodeRef n] -> [NodeRef n] -> PlanT t n m PageNum
    delRefs freed _prev [] = return freed
    delRefs freed prev (ref:rest) = do
      extraFree <- delOrConcreteMat ref
      if freed + extraFree > requiredPages then return $ freed + extraFree
        else delRefs (freed + extraFree) (ref : prev) rest
    delOrConcreteMat ref = do
      canStillDel <- isDeletable ref
      trM $ "Considering deletion: " ++ show (ref,canStillDel)
      if canStillDel then toDel ref else toConcr ref
    toDel :: NodeRef n -> PlanT t n m PageNum
    toDel ref = do
      ref `setNodeStateUnsafe` Concrete Mat NoMat
      totalNodePages ref
    toConcr :: NodeRef n -> PlanT t n m PageNum
    toConcr ref = do
      ref `setNodeStateUnsafe` Concrete Mat Mat
      return 0

reportMatNodes :: Monad m => [NodeRef n] -> PlanT t n m a -> PlanT t n m a
reportMatNodes deleted m = catchError m $ \e -> do
  mat <- nodesInState [Initial Mat,Concrete NoMat Mat,Concrete Mat Mat]
  throwPlan
    $ unlines
      [ashow "Just deleted:",ashow deleted,"Mat nodes:",ashow mat,ashow e]

matNeighbors :: MonadLogic m => NodeRef n -> PlanT t n m ()
matNeighbors node = wrapTrM ("matNeighbors " ++ show node) $ do
  -- Use the fact that it is materialized to materialize the neighbors.
  node `setNodeStateUnsafe` Concrete Mat Mat;
  metaOp <- findPrioritizedMetaOp eitherl node
  trM $ "Materializing: " ++ show (metaOpIn metaOp)
  let depset = toNodeList $ metaOpIn metaOp
  -- XXX: materialize using this, otherwise this will be searching all around
  mapM_ (`setNodeStateSafe` Mat) depset;
  node `setNodeStateUnsafe` Concrete Mat NoMat
  putDelNode node

planSanityCheck
  :: forall t n m .
  MonadLogic m
  => PlanT t n m (Either (PlanSanityError t n) ())
planSanityCheck = runExceptT $ do
  conf <- ask
  let (_,nRefs) = nodeRefs `evalState` def { gbPropNet = propNet conf }
  checkTbl (toNodeList nRefs) MissingSize $ fmap2 fst nodeSizes
  let localBotNodes =
        getBottomNodesN `evalState` def { gbPropNet = propNet conf }
  st <- lift (get :: PlanT t n m (GCState t n))
  lift $ forM_ localBotNodes makeMaterializable
  lift $ put st
  where
    checkTbl
      :: [NodeRef from]
      -> (NodeRef from -> RefMap from to -> PlanSanityError t n)
      -> (GCConfig t n -> RefMap from to)
      -> ExceptT (PlanSanityError t n) (PlanT t n m) ()
    checkTbl refs err extractElem = do
      table <- extractElem <$> lift ask
      forM_ refs $ \nr -> case nr `refLU` table of
        Just _  -> return ()
        Nothing -> throwError $ err nr table

isDeletable :: MonadLogic m => NodeRef n -> PlanT t n m Bool
isDeletable ref = getNodeState ref >>= \case
  Concrete _ Mat -> return False
  _              -> withNoMat ref $ isMaterializable ref

withNoMat :: Monad m => NodeRef n -> PlanT t n m a -> PlanT t n m a
withNoMat = withNodeState (Concrete Mat NoMat) . return
withNodeState :: Monad m => NodeState -> [NodeRef n] -> PlanT t n m a -> PlanT t n m a
withNodeState nodeState refs m = do
  sts <- traverse getNodeState refs
  let setSt = setNodeStateUnsafe' False
  zipWithM_ setSt refs $ repeat nodeState
  ret <- m
  zipWithM_ setSt refs sts
  return ret

-- -- | Get exactly one solution. Schedule this as if it were a single thread.
-- cutPlanT
--   :: MonadLogic m
--   => Proxy h
--   -- -> PlanT t n (HCntT h (Either (PlanningError t n) (a,GCState t n)) []) a
--   -> PlanT t n m a
--   -> PlanT t n m a
-- cutPlanT plan = do
--   st <- get
--   conf <- ask
--   hoistPlanT
--     -- (cutContT
--     (once
--        (runExceptT $ (`runReaderT` conf) $ (`runStateT` st) $ lift3 mzero))
--     plan
