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
  (setNodeMaterialized
  ,setNodeStateSafe
  ,setNodeStateUnsafe
  ,planSanityCheck
  ,newEpoch
  ,withProtected
  ,isMaterializable
  ,makeMaterializable
  ,isMaterialized
  ,matNeighbors
  ,getDependencies
  ,garbageCollectFor
  ,killPrimaries
  ,isDeletable) where

import           Control.Antisthenis.Types
import           Control.Monad.Cont
import           Control.Monad.Except
import           Control.Monad.Extra
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer        hiding (Sum)
import           Data.Bipartite
import           Data.Functor.Identity
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
import           Data.Utils.HContT
import           Data.Utils.ListT
import           Data.Utils.Tup

import           Control.Antisthenis.Lens
import           Control.Arrow
import qualified Control.Category            as C
import           Data.Proxy
import           Data.QueryPlan.Comp
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Types
import           Data.QueryPlan.Utils
import           Data.Utils.Default
import           Data.Utils.Nat


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

setNodeMaterialized :: forall t n m . MonadLogic m => NodeRef n -> PlanT t n m ()
setNodeMaterialized node = wrapTraceT ("setNodeMaterialized " ++ show node) $ do
  sizes <- asks nodeSizes
  traceM $ "Node sizes:" ++  ashow sizes
  -- Populate the metaop cache
  when False $ warmupCache node
  setNodeStateSafe node Mat
  cost <- totalTransitionCost
  trM $ printf "Successfully materialized %s -- cost: %s" (show node) (show cost)

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
      Just (frontierStar -> (deps,_star))
        -> unlessM (allM isConcreteMat $ toNodeList deps) m
    isConcreteMat =
      fmap (\case
              Concrete _ Mat -> True
              _              -> False) . getNodeState

haltPlan :: MonadHaltD m => NodeRef n -> MetaOp t n -> PlanT t n m ()
haltPlan matRef mop = do
  -- From the frontier replace matRef with it's dependencies.
  modify $ \gcs -> gcs{
    frontier=nsDelete matRef (frontier gcs) <> metaOpIn mop}
  extraCost <- metaOpCost [matRef] mop
  haltPlanCost $ fromIntegral $ costAsInt extraCost
haltPlanCost :: MonadHaltD m => Double -> PlanT t n m ()
haltPlanCost concreteCost = do
  frefs <- gets $ toNodeList . frontier
  -- star :: Double <- sum <$> mapM getAStar frefs
  (costs,extraNodes) <- runWriterT $ forM frefs $ \ref -> do
    cost <- lift
      $ wrapTrace ("planCost " ++ ashow ref)
      $ getCost @CostTag Proxy mempty ForceResult ref
    case cost of
      Nothing -> return zero
      Just c -> do
        maybe (return ()) tell $ pcPlan c
        return $ pcCost c
  let star :: Double = sum [fromIntegral $ costAsInt c | c <- costs]
  histCosts <- takeListT 0 $ pastCosts extraNodes
  trM $ printf "Halt%s: %s" (show frefs) $ show (concreteCost,star,histCosts)
  halt $ PlanSearchScore concreteCost (Just star)
  trM "Resume!"

setNodeStateSafe :: MonadLogic m => NodeRef n -> IsMat -> PlanT t n m ()
setNodeStateSafe n = setNodeStateSafe' (findPrioritizedMetaOp lsplit n) n
{-# INLINE setNodeStateSafe' #-}

setNodeStateSafe' :: MonadLogic m =>
                    PlanT t n m (MetaOp t n)
                  -> NodeRef n -> IsMat -> PlanT t n m ()
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
        Mat   -> bot "Tried to set concrete"
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
              cutPlanT $ garbageCollectFor $ node : interm
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
          -- XXX: setNodeNoMatHistoricalCosts node
          delDepMatCache node
          isMaterializable node
            >>= guardl ("not materializable, can't delete" ++ ashow node)
          node `setNodeStateUnsafe` Concrete Mat NoMat
          putDelNode node

-- | Set a list of nodes to Mat state in an order that is likely to
-- require the least budget.
setNodesMatSafe :: MonadLogic m => [NodeRef n] -> PlanT t n m ()
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

withProtected :: MonadLogic m =>
                [NodeRef n] -> PlanT t n m a -> PlanT t n m a
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
  let epoch@GCEpoch{..} NEL.:| rest = epochs st
  if (epoch, protected) `HS.member` epochFilter st
    then bot "Epoch already encountered"
    else let
      epochFilter' = HS.insert (epoch, protected) $ epochFilter st
      -- demote all except the protected
      maybeDemote r x = case x of
            Concrete _ mat -> if fromMaybe 0 (r `refLU` protected) > 0
                             then return x
                             else tell [(r, mat)] >> return (Initial mat)
            _ -> return x
      (newStates, updatedRefs) = runWriter $ refTraverseWithKey maybeDemote nodeStates
    in if newStates == nodeStates
       then bot "No change in node states."
       else do
         put $ st{
           epochs=GCEpoch{nodeStates=newStates,transitions=[]} NEL.:| (epoch:rest),
           epochFilter=epochFilter'
           }
         trM $ "New epoch. Demoted refs: " ++ show updatedRefs

eitherlReader :: BotMonad m =>
                ReaderT a (PlanT t n m) x
              -> ReaderT a (PlanT t n m) x
              -> ReaderT a (PlanT t n m) x
eitherlReader = splitProvenance (EitherLeft,EitherRight) id eitherl'
lsplitReader :: MonadPlus m =>
                ReaderT a (PlanT t n m) x
              -> ReaderT a (PlanT t n m) x
              -> ReaderT a (PlanT t n m) x
lsplitReader = splitProvenance (SplitLeft,SplitRight) id
  $ \(ReaderT l) (ReaderT r) -> ReaderT $ \s -> mplusPlanT (l s) (r s)


garbageCollectFor
  :: forall t n m . MonadLogic m => [NodeRef n] -> PlanT t n m ()
garbageCollectFor ns = wrapTrM ("garbageCollectFor " ++ show ns) $ withGC $ do
  lift preReport
  go `eitherlReader` (lift newEpoch >> go)
  lift $ trM $ "Finished GC to make " ++ show ns
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
        neededPages <- sum <$> traverse totalNodePages ns
        runReaderT (whenM isOversized m) neededPages
        setGC False
      where
        setGC b = modify $ \x -> x { garbageCollecting = b }
    go :: ReaderT PageNum (PlanT t n m) ()
    go = do
      assertGcIsPossible
      killIntermediates `lsplitReader` lift top
      whenM isOversized $ killPrimaries `eitherlReader` lift top
      whenM isOversized $ lift $ bot $ "GC failed: " ++ show ns

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
assertGcIsPossible :: MonadPlus m => ReaderT PageNum (PlanT t n m) ()
assertGcIsPossible = do
  neededPages :: PageNum <- ask
  budg <- budget <$> lift ask
  let sizeOfSt sts = lift $ fmap sum . traverse totalNodePages
        =<< nodesInState sts
  concr <- sizeOfSt [Concrete Mat Mat,Concrete NoMat Mat]
  ini <- sizeOfSt [Initial Mat]
  when (maybe False (neededPages + concr >) budg) $ lift
    $ bot
    $ printf "assertGcIsPossible : needed=%d, concrete=%d, budget=%s, ini=%d"
    neededPages concr (show budg) ini

killIntermediates :: forall t n m . MonadLogic m =>
                    ReaderT PageNum (PlanT t n m) ()
killIntermediates = do
  matInterm <- filterM (lift . fmap not . isProtected)
              =<< filterM (lift . isMaterialized)
              =<< toNodeList . intermediates
              <$> lift ask
  forM_ matInterm $ \x -> lift $ do
    delDepMatCache x
    x `setNodeStateUnsafe` Concrete Mat NoMat
    putDelNode x

isOversized :: forall t n m . MonadLogic m =>
              ReaderT PageNum (PlanT t n m) Bool
isOversized = do
  nsize <- ask
  totalSize <- lift getDataSize
  budget <- budget <$> lift ask
  if maybe False (\b -> nsize + totalSize > b) budget
    then do
      lift $ trM
        $ printf "oversized: %d / %s (needed: %d)"
        totalSize (maybe "<unbounded>" show budget) nsize
      return True
    else return False

killPrimaries :: MonadLogic m => ReaderT PageNum (PlanT t n m) ()
killPrimaries = hoist (wrapTrM "killPrimaries") $ (safeDelInOrder =<<) $ lift $ do
  nsUnordMaybeIsolatedInterm <- nodesInState [Initial Mat]
  nsUnordMaybeInterm <- filterM isDeletable nsUnordMaybeIsolatedInterm
  nsUnord <- filterM (fmap not . isIntermediate) nsUnordMaybeInterm
  guardl "No killable primaries" $ not $ null nsUnord
  -- XXX: We are having no priority in deleting these.
  return nsUnord

safeDelInOrder :: MonadLogic m =>
                 [NodeRef n] -> ReaderT PageNum (PlanT t n m) ()
safeDelInOrder nsOrd = hoist (wrapTrM $ "safeDelInOrder " ++ show nsOrd)
  $ foldr
  delRef
  (lift . guardl "still oversized " =<< isOversized)
  nsOrd
  where
    delRef ref rest = do
      delOrConcreteMat ref
      whenM isOversized rest
    delOrConcreteMat ref = do
      beforeSize <- lift getDataSize
      -- We could semantically use eitherl but eitherl cuts the
      -- computation and does not allow for other paths to be
      -- searched.
      canStillDel <- lift $ isDeletable ref
      lift $ if canStillDel
        then do
          delCost <- transitionCost $ DelNode ref
          haltPlanCost $ fromIntegral $ costAsInt delCost
          ref `setNodeStateSafe` NoMat
        else ref `setNodeStateUnsafe` Concrete Mat Mat
      assertGcIsPossible
      afterSize <- lift getDataSize
      lift $ guardl "We Deletion did more harm than good"
        $ beforeSize >= afterSize

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

planSanityCheck :: forall t n m . MonadLogic m =>
                  PlanT t n m (Either (PlanSanityError t n) ())
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
-- | Get exactly one solution. Schedule this as if it were a single thread.
cutPlanT :: (v ~ HValue m,MonadLogic m) =>
           PlanT t n (HContT v (Either (PlanningError t n) (a,GCState t n)) []) a
         -> PlanT t n m a
cutPlanT plan = do
  st <- get
  conf <- ask
  hoistPlanT
    (cutContT
       (runExceptT $ (`runReaderT` conf) $ (`runStateT` st) $ lift3 mzero))
    plan
