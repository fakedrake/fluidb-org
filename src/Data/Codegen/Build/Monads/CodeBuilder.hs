{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-orphans -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.Monads.CodeBuilder
  ( MonadReadScope(..)
  , MonadCodeBuilder(..)
  , MonadCodeError
  , QueryCppConf(..)
  , CodeBuilderT'
  , MonadSoftCodeBuilder(..)
  , MonadCheckpoint(..)
  , MonadCodeCheckpoint
  , CodeBuildErr(..)
  , CBState(..)
  , MonadWriteScope(..)
  , SoftCodeBuilderT
  , ScopeEnv
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
  , hoistCodeBuilderT'
  , putQueryCppConf
  , putQueryFile
  , runSoftCodeBuilder
  , tellClassM
  , tellFunctionM
  , tellInclude
  , throwCodeErr
  , tryInsert
  ) where

import           Control.Monad.Except
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types
import           Data.CnfQuery.Types
import           Data.Codegen.Build.Monads.Class
import           Data.List
import           Data.Query.SQL.FileSet
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable

import           Control.Monad.State
import           Data.Maybe
import           Data.NodeContainers
import           Data.Utils.MTL
import           Prelude                         hiding (exp)

-- |A code builder monad that can throw errors, carries the code
-- building state, has access to a graph and is on the leaf of a plan.
type CodeBuilderT' e s t n m = ExceptT (CodeBuildErr e s t n)
                               (StateT (CBState e s t n)
                                (CGraphBuilderT e s t n m))
hoistCodeBuilderT' :: Monad m =>
                     (forall a . m a -> g a)
                   -> CodeBuilderT' e s t n m a
                   -> CodeBuilderT' e s t n g a
hoistCodeBuilderT' f = hoist (hoist (hoistCGraphBuilderT f))

type SoftCodeBuilderT e s t n m = StateT Int (CodeBuilderT' e s t n m)

-- |Embed the query cache operations to the codebuilder monad
putQueryFile :: MonadCodeBuilder e s t n m => CNFQuery e s -> FileSet -> m ()
putQueryFile q f = do
  st <- getCBState
  putCBState st{
    cbQueryFileCache=putCachedFile (cbQueryFileCache st) q f}

delQueryFile :: MonadCodeBuilder e s t n m => CNFQuery e s -> m ()
delQueryFile q = do
  st <- getCBState
  putCBState st{cbQueryFileCache=delCachedFile (cbQueryFileCache st) q}

delNodeFile
  :: (Hashables2 e s
     ,MonadError (CodeBuildErr e s t n) m
     ,MonadCodeBuilder e s t n m
     ,MonadReader (ClusterConfig e s t n) m)
  => NodeRef n
  -> m ()
delNodeFile ref =
  dropState (ask,const $ return ()) (getNodeCnfN ref) >>= mapM_ delQueryFile

getNodeFile
  :: (Hashables2 e s
     ,MonadCodeBuilder e s t n m
     ,MonadError (CodeBuildErr e s t n) m
     ,MonadReader (ClusterConfig e s t n) m)
  => NodeRef n
  -> m (Maybe FileSet)
getNodeFile n = dropState (ask,const $ return ()) (getNodeCnfN n) >>= \case
  [] -> throwAStr $ "No file for node: " ++ ashow n
  cnfs -> do
    queryToFile <- getCachedFile . cbQueryFileCache <$> getCBState
    case nub $ queryToFile <$> cnfs of
      [filename] -> return filename
      names
        -> throwAStr $ "Expected one filename for node: " ++ ashow (n,names)

-- | Create a filename for writing. Throw if there already is a file
-- for the query corresponding to the node.
mkNodeFile
  :: forall e s t n m constr .
  (FileSetConstructor constr
  ,Hashables2 e s
  ,MonadCodeBuilder e s t n m
  ,MonadError (CodeBuildErr e s t n) m
  ,MonadReader (ClusterConfig e s t n) m)
  => constr
  -> NodeRef n
  -> m FileSet
mkNodeFile constr n =
  dropState (ask,const $ return ()) (getNodeCnfN n) >>= \case
    [] -> error $ "No cnfs for node " ++ show n
    cnfs -> getNodeFile n >>= \case
      Just fn -> throwAStr $ "Overwriting file" ++ ashow (n,cnfs,fn)
      Nothing -> do
        f <- mkFileName constr n cnfs
        mapM_ (`putQueryFile` f) cnfs
        return f

mkFileName :: (FileSetConstructor constr, MonadCodeBuilder e s t n m,
              MonadError (CodeBuildErr e s t n) m) =>
             constr
           -> NodeRef n
           -> [CNFQuery e s]
           -> m FileSet
mkFileName constr n cnfs = do
  qf <- getCachedFile . defaultQueryFileCache . cbQueryCppConf <$> getCBState
  let assoc = (\cnf -> (qf cnf,cnf)) <$> cnfs
  fileset <- case nub $ mapMaybe fst assoc of
              []  -> return $ constructFileSet constr (runNodeRef n)
              [f] -> return f
              xs  -> throwAStr $ "Conflicting files for node: " ++ ashow (n,xs)
  mapM_ (`putQueryFile` fileset) $ snd <$> filter (isNothing . fst) assoc
  return fileset

instance Monad m => MonadCheckpoint (CodeBuilderT' e s t n m) where
  type CheckpointType (CodeBuilderT' e s t n m) = CBState e s t n
  getCheckpoint = get
  {-# INLINE getCheckpoint #-}
  restoreCheckpoint = put
instance Monad m => MonadCheckpoint (SoftCodeBuilderT e s t n m) where
  type CheckpointType (SoftCodeBuilderT e s t n m) =
    (Int, CheckpointType (CodeBuilderT' e s t n m))
  restoreCheckpoint (a, b) = put a >> lift (restoreCheckpoint b)
  {-# INLINE restoreCheckpoint #-}
  getCheckpoint = do
    c <- get
    c' <- lift getCheckpoint
    return (c, c')
  {-# INLINE getCheckpoint #-}

clusterLiftCB :: Monad m =>
                CGraphBuilderT e s t n m a -> CodeBuilderT' e s t n m a
clusterLiftCB = lift2
