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
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.MTL


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
  => AnyCluster e s t n
  -> m (CC.Statement CC.CodeSymbol)
triggerCode clust = do
  constr <- dropReader (asks fst) $ clusterCall ForwardTrigger clust
  ioFiles <- clustToIoFiles ForwardTrigger clust
  let ioFilesD = IOFilesD { iofCluster = ioFiles,iofDir = ForwardTrigger }
  case constrBlock constr ioFilesD of
    Just x -> return x
    Nothing -> throwAStr
      $ "Missing input files: " ++ ashow (clusterInputs clust)

-- |Create code that triggers the query in reverse. The IOFiles
-- provided should already be reversed.
-- New implementation
revTriggerCode
  :: forall e s t n m .
  (MonadReader (ClusterConfig e s t n,GCState t n) m
  ,MonadCodeCheckpoint e s t n m)
  => AnyCluster e s t n
  -> m (CC.Statement CC.CodeSymbol)
revTriggerCode clust = do
  constr <- dropReader (asks fst) $ clusterCall ReverseTrigger clust
  ioFiles <- clustToIoFiles ReverseTrigger clust
  let ioFilesD = IOFilesD { iofCluster = ioFiles,iofDir = ReverseTrigger }
  case constrBlock constr ioFilesD of
    Just x -> return x
    Nothing -> do
      throwAStr
        $ "Missing input files: " ++ ashow (clusterOutputs clust,ioFiles)
