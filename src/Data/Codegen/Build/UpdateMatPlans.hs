{-# LANGUAGE CPP                 #-}
{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
module Data.Codegen.Build.UpdateMatPlans
  ( getMatPlan
  , delMatPlan
  , triggerCluster
  ) where

import Data.Utils.MTL
import Data.Utils.Functors
import Data.Utils.Hashable
import           Control.Monad.State
import           Data.Utils.AShow
import           Data.Cluster.Propagators
import           Data.Cluster.Types
import           Data.Codegen.Build.Monads
import           Data.Utils.Debug
import           Data.NodeContainers

getMatPlan :: (Hashables2 e s, Monad m) =>
             NodeRef n -> CodeBuilderT e s t n m (QueryPlan e s)
getMatPlan ref = wrapTrace ("getMatPlan " ++ ashow ref) $ dropReader (lift2 get)
  $ getDefaultingFull <$> getNodePlan ref >>= \case
    Nothing -> lift2 $ throwAStr $ "getMatPlan: no plan for node: "
        ++ ashow ref
    Just x -> return x

delMatPlan :: (Hashables2 e s, Monad m) =>
             NodeRef n -> CodeBuilderT e s t n m ()
delMatPlan = lift2 . delNodePlan

triggerCluster :: (Hashables2 e s, Monad m) =>
                 AnyCluster e s t n -> CodeBuilderT e s t n m ()
triggerCluster = lift2 . triggerClustPropagator
