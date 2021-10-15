{-# LANGUAGE CPP                 #-}
{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
module Data.Codegen.Build.UpdateMatShapes
  (getMatShape
  ,triggerCluster
  ,internalSyncShape
  ,promoteNodeShape
  ,demoteNodeShape) where

import           Control.Monad.State
import           Data.Cluster.Propagators
import           Data.Cluster.Types
import           Data.Codegen.Build.Monads
import           Data.NodeContainers
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL

getMatShape
  :: (Hashables2 e s,Monad m)
  => NodeRef n
  -> CodeBuilderT e s t n m (QueryShape e s)
getMatShape ref =
  wrapTrace ("getMatShape " ++ ashow ref)
  $ dropReader (lift2 get)
  $ getNodeShape ref >>= guardMaybe . getDefaultingFull
  where
    guardMaybe = \case
      Nothing
       -> lift2 $ throwAStr $ "getMatShape: no shape for node: " ++ ashow ref
      Just x -> return x

triggerCluster
  :: (Hashables2 e s,Monad m)
  => AnyCluster e s t n
  -> CodeBuilderT e s t n m (ShapeCluster NodeRef e s t n)
triggerCluster = lift2 . triggerClustPropagator

promoteNodeShape
  :: (Hashables2 e s,Monad m)
  => NodeRef n
  -> CodeBuilderT e s t n m (Defaulting (QueryShape e s))
promoteNodeShape ref = do
  dropState (lift3 get,lift2 . put) $ modNodeShape ref promoteDefaulting
  dropReader (lift2 get) $ getNodeShape ref

promoteNodeShape
  :: (Hashables2 e s,Monad m)
  => NodeRef n
  -> CodeBuilderT e s t n m (Defaulting (QueryShape e s))
promoteNodeShape ref = do
  dropState (lift3 get,lift2 . put) $ modNodeShape ref promoteDefaulting
  dropReader (lift2 get) $ getNodeShape ref

-- | Internal synchronization: the full schema is carried to the
-- default schema but the sizes are not touched.
internalSyncState
  :: (Hashables2 e s,Monad m)
  => NodeRef n
  -> CodeBuilderT e s t n m (Defaulting (QueryShape e s))
internalSyncShape ref = do
  dropState (lift3 get,lift2 . put) $ modNodeShape ref _
  dropReader (lift2 get) $ getNodeShape ref

demoteNodeShape
  :: (Hashables2 e s,Monad m)
  => NodeRef n
  -> CodeBuilderT e s t n m (Defaulting (QueryShape e s))
demoteNodeShape ref = do
  dropState (lift3 get,lift2 . put) $ modNodeShape ref demoteDefaulting
  dropReader (lift2 get) $ getNodeShape ref
