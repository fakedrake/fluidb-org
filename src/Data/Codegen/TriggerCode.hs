{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Codegen.TriggerCode
  ( triggerCode
  , revTriggerCode
  , delCode
  ) where

import           Control.Monad.Reader
import           Data.Cluster.Types
import           Data.Codegen.Build.Constructors
import           Data.Codegen.Build.IoFiles
import           Data.Codegen.Build.IoFiles.Types
import           Data.Codegen.Build.Monads.Class
import qualified Data.CppAst                      as CC
import           Data.NodeContainers
import           Data.QueryPlan.Types
import           Data.Utils.MTL
import           Data.Utils.Tup


delCode :: (Applicative m,ConstructorArg fileLike)
        => fileLike
        -> m (CC.Statement CC.CodeSymbol)
delCode fp = pure
  $ CC.ExpressionSt
  $ CC.FunctionAp "deleteFile" []
  [toConstrArg fp]
triggerCode
  :: forall e s t n m .
  (MonadReader (ClusterConfig e s t n,GCState t n) m
  ,MonadCodeCheckpoint e s t n m)
  => Tup2 [NodeRef n]
  -> AnyCluster e s t n
  -> m (CC.Statement CC.CodeSymbol)
triggerCode io clust = do
  constr <- dropReader (asks fst) $ clusterCall ForwardTrigger clust
  ioFiles <- clustToIoFiles io ForwardTrigger clust
  let ioFilesD = IOFilesD { iofCluster = ioFiles,iofDir = ForwardTrigger }
  constrBlock constr ioFilesD

-- |Create code that triggers the query in reverse. The IOFiles
-- provided should already be reversed.
-- New implementation
revTriggerCode
  :: forall e s t n m .
  (MonadReader (ClusterConfig e s t n,GCState t n) m
  ,MonadCodeCheckpoint e s t n m)
  => Tup2 [NodeRef n]
  -> AnyCluster e s t n
  -> m (CC.Statement CC.CodeSymbol)
revTriggerCode ioFwd clust = do
  constr <- dropReader (asks fst) $ clusterCall ReverseTrigger clust
  ioFiles <- clustToIoFiles (swapTup2 ioFwd) ReverseTrigger clust
  let ioFilesD = IOFilesD { iofCluster = ioFiles,iofDir = ReverseTrigger }
  constrBlock constr ioFilesD
