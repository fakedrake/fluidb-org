{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE ViewPatterns          #-}
{-# OPTIONS_GHC -Wno-name-shadowing -Wno-unused-top-binds -Wno-deprecations #-}


module Data.QueryPlan.MetaOp
  ( findPrioritizedMetaOp
  , findMetaOps
  , findCostedMetaOps
  , findOnIn
  , findOnOut
  , nubMetaOp
  , metaOpCost
  , getHardBudget
  , findTriggerableMetaOps
  , metaOpNeededPages
  ) where

import           Control.Applicative
import           Control.Monad.Cont
import           Control.Monad.Extra
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bipartite
import           Data.Function
import           Data.List
import           Data.List.Extra
import           Data.Maybe
import           Data.Monoid
import           Data.NodeContainers
import           Data.QueryPlan.MetaOpCache
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Transitions
import           Data.Utils.Debug
import           Data.Utils.Functors
import           Data.Utils.ListT
import           Data.Utils.Ranges
import           Prelude                    hiding (filter, lookup)

import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.Types
import           Data.Utils.AShow.Print     (ashowLine)
import           Data.Utils.Default
import           Data.Utils.Unsafe

getNodeLinksT' :: Monad m =>
                 Side
               -> [IsReversible]
               -> NodeRef t
               -> GraphBuilderT t n m (NodeSet n)
getNodeLinksT' s rev ref = getNodeLinksT (NodeLinksFilter ref [s] rev) <&> \case
  Nothing -> error $ printf "Non-existent t-node: %n" ref
  Just x  -> x
{-# INLINE getNodeLinksT' #-}

getNodeLinksN'
  :: Monad m
  => Side
  -> [IsReversible]
  -> NodeRef n
  -> GraphBuilderT t n m (NodeSet t)
getNodeLinksN' s rev ref = getNodeLinksN (NodeLinksFilter ref [s] rev) >>= \case
  Nothing -> do
    (_,ns) <- nodeRefs
    error $ printf "Non-existent n-node: %n (n-nodes: %s)" ref (show ns)
  Just x -> return x
{-# INLINE getNodeLinksN' #-}

nubMetaOp :: [MetaOp t n] -> [MetaOp t n]
nubMetaOp = nubOn $ \MetaOp{..} -> (metaOpIn, metaOpOut, metaOpInterm)

-- | We assume that tables show In -> Out relations
type InSet n = NodeSet n
type OutSet n = NodeSet n
type InRef n = NodeRef n
type OutRef n = NodeRef n

-- Assert that a is not an intermediate
-- Get a set of invalid MetaOps that satisfy (a in metaOpOut)
--   Note that the way to extend inputs is \i -> findOnOut i
--   Note that the way to register them is
-- Get a set of invalid MetaOps that satisfy (a in metaOpIn) and invert
--   Note that the way to extend inputs is \i -> findOnIn i
-- for each metaop

data MetaOpFollow t n =
  MetaOpFollow
  { refOnOut
      :: forall m .
      Monad m
      => NodeRef n
      -> PlanT t n m [(InSet n,OutSet n,NodeRef t)]
   ,triggerTRef
      :: forall m . Monad m => NodeRef t -> PlanT t n m (Transition t n)
  }

-- |`followUnsafe followIns` will result in all input nodes (in
-- absolute terms) to be non-intermediates
followIns :: MetaOpFollow t n
followIns = MetaOpFollow {
  refOnOut=findOnOut,
  triggerTRef=trigger
  }

-- |`followUnsafe followOuts` will result in all output nodes (in
-- absolute terms) to be non-intermediates
followOuts :: MetaOpFollow t n
followOuts = MetaOpFollow {
  refOnOut=fmap2 (\(a,b,c) -> (b,a,c)) . findOnIn,
  triggerTRef=revTrigger
  }

-- | Move nodes from the output section of the metaop to the
-- intermediate section.
cleanOutputs :: Monad m => MetaOp t n -> PlanT t n m (MetaOp t n)
cleanOutputs mop = do
  (interm, out) <- partitionM isIntermediate $ toNodeList $ metaOpOut mop
  return $ mop{
    metaOpOut=fromNodeList out,
    metaOpInterm=metaOpInterm mop <> fromNodeList interm
    }
{-# INLINE cleanOutputs #-}


-- | Expand the inputs but not the outputs until all inputs are
-- non-intermediates. Unsafe because we assume the ref provide is not
-- an intermediate.
followUnsafe
  :: forall m t n .
  Monad m
  => MetaOpFollow t n
  -> NodeRef n
  -> ListT (PlanT t n m) (MetaOp t n)
followUnsafe mopf@MetaOpFollow{..} ref = do
  (insAll, outRefs, tref) <- mkListT $ refOnOut ref
  (metaOpIntermL, fromNodeList -> inRefs) <-
    lift $ partitionM isIntermediate $ toNodeList insAll
  restMopMixedOutp <- fmap mconcat $ followUnsafe mopf `traverse` metaOpIntermL
  (restMop :: MetaOp t n) <- lift $ cleanOutputs restMopMixedOutp
  returnGuard $ restMop <> MetaOp{
    metaOpPlan=mkMetaOpPlan mopf tref,
    metaOpInterm=fromNodeList metaOpIntermL,
    metaOpOut=outRefs `nsDifference` inRefs,
    metaOpIn=inRefs}
  where
    returnGuard mop = do
      guard $ nsMember ref $ metaOpOut mop
      return mop
{-# INLINE followUnsafe #-}

mkMetaOpPlan
  :: Monad m => MetaOpFollow t n -> NodeRef t -> PlanT t n m [Transition t n]
mkMetaOpPlan MetaOpFollow {..} tref = do
  initialTransition <- triggerTRef tref
  actualTrns <- filterTrnsOutputs  initialTransition
  return [actualTrns]

-- | Remove the outputs that are not materialized.
filterTrnsOutputs :: Monad m => Transition t n -> PlanT t n m (Transition t n)
filterTrnsOutputs = \case
  RTrigger x y z -> RTrigger x y <$> onlyMats z
  Trigger x y z  -> Trigger x y <$> onlyMats z
  x              -> pure x
  where
    onlyMats outs = filterM isMaterialized outs >>= \case
      flt@(_:_) -> return flt
      []  -> do
        states <- mapM getNodeState outs
        throwPlan $ "No materialized nodes out of: " ++ ashow (zip outs states)

-- | Problematic when intermediate is the argument. They are returned
findMetaOps :: Monad m => NodeRef n -> PlanT t n m [MetaOp t n]
-- in increasing order of size.
findMetaOps = fmap2 fst . findCostedMetaOps
findMetaOps' :: Monad m => NodeRef n -> PlanT t n m [MetaOp t n]
findMetaOps'
  ref = runListT $ followUnsafe followIns ref <|> followUnsafe followOuts ref
{-# INLINE findMetaOps' #-}

findCostedMetaOps :: Monad m => NodeRef n -> PlanT t n m [(MetaOp t n,Cost)]
findCostedMetaOps = memM (getMetaOpCache,putMetaOpCache) $ \ref -> do
  mops <- findMetaOps' ref
  costedMops <- mapM (\mop -> (mop,) <$> metaOpCost [ref] mop) mops
  return $ sortOn (costAsInt . snd) costedMops

memM :: Monad m => (a -> m (Maybe b),a -> b -> m ()) -> (a -> m b) -> a -> m b
memM (get',put') fn a = get' a >>= \case
  Just b -> return b
  Nothing -> do
    b <- fn a
    put' a b
    return b
{-# INLINE memM #-}
sortOnM :: (Ord o,Monad m) => (a -> m o) -> [a] -> m [a]
sortOnM f = fmap (map fst . sortOn snd) . mapM (\x -> (x,) <$> f x)


-- | Get the full set of inputs and outputs for each t-node.
findOnSide :: forall t n m . Monad m =>
             ([IsReversible], [IsReversible])
           -> GraphBuilderT t n m (NodeSet t)
           -> PlanT t n m [(InSet n, OutSet n, NodeRef t)]
findOnSide (revsIn,revsOut) tNodesM = do
  net <- asks propNet
  tNodes <- liftPlanT $ stripGB net tNodesM
  liftPlanT $ forM (toNodeList tNodes) $ \tnode
    -> let linksOn revs side = stripGB net $ getNodeLinksT' side revs tnode
    in tritup <$> linksOn revsIn Inp <*> linksOn revsOut Out <*> return tnode
  where
    stripGB :: Bipartite t n -> StateT (GBState t n) m a -> m a
    stripGB net = (`evalStateT` def { gbPropNet = net })
    tritup a b c = (a,b,c)
{-# INLINE findOnSide #-}

-- |This is in absolute terms,s NodeRef would be in the InSet
findOnIn :: Monad m =>
           NodeRef n
         -> PlanT t n m [(InSet n, OutSet n, NodeRef t)]
findOnIn = findOnSide (fullRange, [Reversible]) . getNodeLinksN' Out [Reversible]

-- |This is in absolute terms,s NodeRef would be in the OutSet
findOnOut :: Monad m =>
            NodeRef n
          -> PlanT t n m [(InSet n, OutSet n, NodeRef t)]
findOnOut = findOnSide (fullRange,fullRange) . getNodeLinksN' Inp fullRange

findTriggerableMetaOps
  :: forall n t m . MonadLogic m => NodeRef n -> PlanT t n m [MetaOp t n]
findTriggerableMetaOps ref = do
  mops <- findMetaOps ref
  hbM <- getHardBudget
  triggerableMops <- mapM (chooseOuts hbM) mops
  case iterleave triggerableMops of
    [] -> do
      sizedInputs <- mapM (\r -> (r,) <$> getNodeState r)
        $ toNodeList . metaOpIn =<< mops
      bot
        $ "None of the metaops are triggerable: "
        ++ ashowLine (ref,triggerableMops,sizedInputs,mops)
    rs -> return rs
  where
    chooseOuts hbM mop = do
      nonMatableInp <- anyM isConcreteNoMatM $ toNodeList $ metaOpIn mop
      if nonMatableInp then return [] else leanMop ref hbM mop
    isConcreteNoMatM =
      fmap (\case
              Concrete _ NoMat -> True
              _                -> False) . getNodeState
{-# INLINABLE findTriggerableMetaOps #-}

iterleave :: [[a]] -> [a]
iterleave  = go [] where
  go [] []          = []
  go x []           = go [] $ reverse x
  go x ((y:ys0):ys) = y:go (ys0:x) ys
  go x ([]:ys)      = go x ys


getHardBudget :: Monad m => PlanT t n m (Maybe Int)
getHardBudget = do
  budgM <- asks budget
  concr <- fmap sum . mapM totalNodePages
    =<< filterM isProtected
    =<< nodesInState [Concrete Mat Mat,Concrete NoMat Mat]
  return $ (\x -> x - concr) <$> budgM

findPrioritizedMetaOp
  :: forall n t m .
  (HasCallStack,MonadLogic m)
  => (forall a . PlanT t n m a -> PlanT t n m a -> PlanT t n m a)
  -> NodeRef n
  -> PlanT t n m (MetaOp t n)
findPrioritizedMetaOp splitFn ref = do
  mops <- findTriggerableMetaOps ref
  foldr1Unsafe splitFn $ return <$> mops

metaOpNeededPages :: (HasCallStack,Monad m) => MetaOp t n -> PlanT t n m Int
metaOpNeededPages MetaOp {..} = do
  nonMatRefs
    <- filterM (fmap not . isMaterialized) $ toNodeList $ metaOpIn <> metaOpOut
  sizes <- mapM totalNodePages nonMatRefs
  return $ sum sizes

-- | Check if a trigger fits in the budget.
triggerFits :: Monad m => Maybe Int -> MetaOp t n -> PlanT t n m Bool
triggerFits freePages mop = do
  neededPages <- metaOpNeededPages mop
  return $ maybe True (neededPages <) freePages

leanMop
  :: Monad m
  => NodeRef n
  -> Maybe Int
  -> MetaOp t n
  -> PlanT t n m [MetaOp t n]
leanMop ref freePages mop = do
  mopFits <- triggerFits freePages mop
  if mopFits then return [mop]
    else filterM (triggerFits freePages) allMops
  where
    allMops =
      [mop { metaOpOut = fromNodeList $ ref : outs } | outs
      <- subsequences $ filter (ref /=) $ toNodeList $ metaOpOut mop]

metaOpCost :: Monad m => [NodeRef n] -> MetaOp t n -> PlanT t n m Cost
metaOpCost matRefs MetaOp{..} = do
  s <- get
  let setNodeTo m ref = setNodeStateUnsafe' False ref $ Concrete NoMat m
  mapM_ (setNodeTo NoMat) $ toNodeList metaOpOut
  mapM_ (setNodeTo Mat)
    $ filter (`nsMember` metaOpOut) matRefs
    ++ toNodeList metaOpIn
    ++ toNodeList metaOpInterm
  transitions <- metaOpPlan
  ret <- mconcat <$> mapM transitionCost transitions
  put s
  return ret
