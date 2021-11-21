-- | Here are some deprecated functions from Solve.hs
module SolveUtils () where

import           Data.QueryPlan.Dependencies

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
