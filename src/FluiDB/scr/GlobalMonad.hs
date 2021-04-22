{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

module Database.FluiDB.Running.GlobalMonad (toGlobalSolve,joinGST) where

import           Control.Monad.Except
import           Control.Monad.Morph
import           Control.Monad.State
import           Data.Functor.Identity
import           Database.FluiDB.BipartiteGraph
import           Database.FluiDB.Cluster.Types
import           Database.FluiDB.Codegen.Build.Monads.CodeBuilder
import           Database.FluiDB.Optimizations
import           Database.FluiDB.QueryPlan
import           Database.FluiDB.Running.Types
import           Database.FluiDB.Utils

joinGST :: Monad m =>
          (GlobalSolveT e s t n (GlobalSolveT e s t n m)) a
        -> (GlobalSolveT e s t n m) a
joinGST = (>>= either throwError return)
          . dropState (get,put)
          . runExceptT
dropGlobalSt :: (MonadState (GlobalConf e s t n) m, IsGlobalConf e s t n st) =>
               StateT st m a -> m a
dropGlobalSt = dropState (getGlobalConf <$> get,modify . modGlobalConf)
dropGlobalErr :: (MonadError (GlobalError e s t n) m,
                 IsGlobalError e s t n err) =>
                ExceptT err m a -> m a
dropGlobalErr = dropExcept $ throwError . toGlobalError

class GlobalMonad e s t n m m' | m' -> m, m' -> t, m' -> n where
  toGlobalSolve :: m' a -> GlobalSolveT e s t n m a
instance Monad m => GlobalMonad e s t n m (PlanT t n m) where
  toGlobalSolve p = errLift
    $ dropReader (globalGCConfig <$> get)
    $ dropGlobalSt
    $ hoist (hoist (hoist lift)) p
instance Monad m => GlobalMonad e s t n m (GraphBuilderT t n m) where
  toGlobalSolve = dropGlobalSt . hoist lift2
instance Monad m => GlobalMonad e s t n m (CGraphBuilderT e s t n m) where
  toGlobalSolve = dropGlobalSt . dropGlobalErr . hoist (hoist toGlobalSolve)
instance Monad m => GlobalMonad e s t n m (CodeBuilderT' e s t n m) where
  toGlobalSolve = dropGlobalSt . dropGlobalErr . hoist (hoist toGlobalSolve)
instance GlobalMonad e s t n Identity (Either (OptimizationError e s t n)) where
  toGlobalSolve = either (throwError . toGlobalError) return
