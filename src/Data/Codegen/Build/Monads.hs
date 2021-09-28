{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.Monads
  ( MonadReadScope(..)
  , MonadCodeBuilder(..)
  , MonadCodeError
  , QueryCppConf(..)
  , MonadSoftCodeBuilder(..)
  , MonadCheckpoint(..)
  , MonadCodeCheckpoint
  , CodeBuildErr(..)
  , CBState(..)
  , MonadWriteScope(..)
  , SoftCodeBuilderT
  , ScopeEnv(..)
  , CodeBuilderT
  , QueryShape
  , MonadSchemaScope
  , throwCodeErrStr
  , clusterLiftCB
  , delNodeFile
  , delQueryFile
  , emptyCBState
  , evalAggrEnv
  , evalQueryEnv
  , getClassNames
  , getNodeFile
  , getQueries
  , getQueryCppConf
  , getScopeQueries
  , mkFileName
  , mkNodeFile
  , putQueryCppConf
  , putQueryFile
  , runSoftCodeBuilder
  , tellClassM
  , tellFunctionM
  , tellInclude
  , throwCodeErr
  , tryInsert
  , hoistCodeBuilderT
  , getGCState
  , getGBState
  , getClusterConfig
  ) where

import           Control.Monad.State.Class
import           Control.Monad.Trans
import           Data.Bipartite
import           Data.Cluster.Types
import           Data.Codegen.Build.Monads.Class
import           Data.Codegen.Build.Monads.CodeBuilder
import           Data.QueryPlan.Types
import           Data.Utils.Functors

type CodeBuilderT e s t n m = CodeBuilderT' e s t n (PlanT t n m)
getGCState :: Monad m => CodeBuilderT e s t n m (GCState t n)
getGCState = lift4 $ lift get
getGBState :: Monad m => CodeBuilderT e s t n m (GBState t n)
getGBState = lift4 get
getClusterConfig :: Monad m => CodeBuilderT e s t n m (ClusterConfig e s t n)
getClusterConfig = lift3 get

hoistCodeBuilderT :: Monad m =>
                    (forall a . m a -> g a)
                  -> CodeBuilderT e s t n m a
                  -> CodeBuilderT e s t n g a
hoistCodeBuilderT f = hoistCodeBuilderT' (hoistPlanT f)
