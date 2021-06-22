{-# LANGUAGE CPP                   #-}
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

module Data.QueryPlan.Utils
  ( lsplit
  , maximumSafe
  , sbreak
  , splitM
  , gbToPlan
  , wrapTrM
  , foldPlan
  , eitherl
  , splitProvenance
  , guardlM
  , guardl
  ) where

import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.BipartiteGraph
import           Data.NodeContainers
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Unsafe

lsplit :: MonadLogic m => PlanT t n m a -> PlanT t n m a -> PlanT t n m a
lsplit = splitProvenance (SplitLeft, SplitRight) id mplusPlanT
eitherl :: MonadLogic m => PlanT t n m a -> PlanT t n m a -> PlanT t n m a
eitherl = splitProvenance (EitherLeft,EitherRight) id eitherl'

splitProvenance :: (Monad m, MonadState (GCState t n) m0) =>
                  (ProvenanceAtom, ProvenanceAtom)
                -> (m0 () -> m ()) -> (m a -> m a -> m a)
                -> m a -> m a -> m a
splitProvenance (latom,ratom) lift' app l r = do
  let pushSt b = lift' $ modify $ \st -> st{provenance=b:provenance st}
  (pushSt latom >> l) `app` (pushSt ratom >> r)
{-# INLINE splitProvenance #-}

sbreak :: forall a m . Monad m =>
         (NodeRef a -> m Bool) -> NodeSet a -> m (NodeSet a, NodeSet a)
sbreak f = foldM go (mempty, mempty) . toNodeList where
  go :: (NodeSet a, NodeSet a) -> NodeRef a -> m (NodeSet a, NodeSet a)
  go (y, n) x = do
    predic <- f x
    return $ if predic then (x `nsInsert` y, n) else (y, x `nsInsert` n)

wrapTrM :: Monad m => String -> PlanT t n m a -> PlanT t n m a
wrapTrM msg m = do
  trM $ "[Before] " ++ msg
  ret <- m `withCleanupMsg` ("[After:fail] " ++ msg)
  trM $ "[After] " ++ msg
  return ret

withCleanupMsg :: MonadState (GCState t n) m => m a -> String -> m a
withCleanupMsg m msg = do
  msgTr <- gets traceDebug
  modify $ \st -> st{traceDebug=msg:msgTr}
  ret <- m
  msgTr' <- gets traceDebug
  if msgTr' == msg:msgTr
    then return ()
    else error $ printf "expected %s got %s" (ashow msgTr) (ashow $ tail msgTr')
  modify $ \st -> st{traceDebug=tail msgTr'}
  return ret

gbToPlan :: MonadReader (GCConfig t n) m => GraphBuilder t n a -> m a
gbToPlan gb = do
  graph <- asks propNet
  return $ evalState gb mempty{gbPropNet=graph}

splitM :: MonadPlus m => [a] -> (a -> m b) -> m b
splitM a f = go a where
  go [] = mzero
  go [x] = f x
  go (x:xs) = f x `mplus` rest where
    rest = go xs


foldPlan :: MonadLogic m =>
           (PlanT t n m a -> PlanT t n m a -> PlanT t n m a)
         -> [PlanT t n m a]
         -> PlanT t n m a
foldPlan _ []       = bot "foldPlan"
foldPlan splitFn ms = foldr1Unsafe splitFn ms

guardlM :: MonadLogic m => String -> PlanT t n m Bool -> PlanT t n m ()
guardlM msg = (>>= guardl msg)
guardl :: MonadLogic m => String -> Bool -> PlanT t n m ()
guardl msg p = if p then top else bot $ "guardl: " ++ msg

maximumSafe :: Ord a => [a] -> Maybe a
maximumSafe [] = Nothing
maximumSafe xs = Just $ maximum xs
