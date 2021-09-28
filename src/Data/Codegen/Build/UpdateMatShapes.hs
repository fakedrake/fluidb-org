{-# LANGUAGE CPP                 #-}
{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
module Data.Codegen.Build.UpdateMatShapes
  (getMatShape
  ,delMatShape
  ,triggerCluster) where

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

getMatShape :: (Hashables2 e s, Monad m) =>
             NodeRef n -> CodeBuilderT e s t n m (QueryShape e s)
getMatShape ref =
  wrapTrace ("getMatShape " ++ ashow ref)
  $ dropReader (lift2 get)
  $ getNodeShape ref >>= guardMaybe . getDefaultingFull
  where
    guardMaybe = \case
      Nothing
       -> lift2 $ throwAStr $ "getMatShape: no shape for node: " ++ ashow ref
      Just x -> return x

delMatShape
  :: (Hashables2 e s,Monad m) => NodeRef n -> CodeBuilderT e s t n m ()
delMatShape = lift2 . delNodeShape

triggerCluster
  :: (Hashables2 e s,Monad m)
  => AnyCluster e s t n
  -> CodeBuilderT e s t n m (ShapeCluster NodeRef e s t n)
triggerCluster = lift2 . triggerClustPropagator
