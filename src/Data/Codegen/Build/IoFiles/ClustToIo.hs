{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.IoFiles.ClustToIo
  ( getClusterN
  , getClusterT
  , toFiles
  , clustToIoFiles
  ) where

import Data.QueryPlan.Nodes
import Data.Utils.MTL
import Data.Utils.Unsafe
import Data.Utils.Functors
import Data.Cluster.ClusterConfig
import Data.CnfQuery.Types
import Data.Utils.Hashable
import Data.QueryPlan.Types
import Data.Utils.Compose
import Data.Cluster.Types
import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Either
import           Data.Maybe
import           Data.Tuple
import           Data.Void
import           Data.Utils.AShow
import           Data.Codegen.Build.IoFiles.MkFiles
import           Data.Codegen.Build.IoFiles.Types
import           Data.Codegen.Build.Monads.CodeBuilder
import           Data.NodeContainers
import           Data.Query.QuerySchema
import           Prelude                                          hiding (exp)

-- | Pattern match on the query and cluster that contains the
-- filepaths to generate code. (output, input)
toFiles :: IOFilesG interm inp out -> ([Maybe out], [Maybe inp])
toFiles = swap . splitIO . toList where
  splitIO :: [NodeRole a b c] -> ([b], [c])
  splitIO = partitionEithers . toEithers where
    toEithers []                  = []
    toEithers (Input x:xs)        = Left x:toEithers xs
    toEithers (Output x:xs)       = Right x:toEithers xs
    toEithers (Intermediate _:xs) = toEithers xs

type CanPutIdLocal c op e =
  CanPutIdentity
    (c Identity (Compose ((,) (Maybe (op e))) Identity))
    (c Identity Identity)
    Identity
    (Compose ((,) (Maybe (op e))) Identity)

matMaybe :: MonadReader (ClusterConfig e s t n,GCState t n) m
         => NodeRef n
         -> m (Maybe (NodeRef n))
matMaybe ref = dropReader (snd <$> ask) $ getNodeStateReader ref <&> \case
  Concrete _ Mat -> Just ref
  Initial Mat -> Just ref
  _ -> Nothing

-- | Turn the ends of a cluster into (Plan,File) pairs. This checks
-- first if the at the edge is (to be) materialized and puts Nothing
-- if it isn't.
clustToIoFiles
  :: forall e s t n m .
  (Hashables2 e s
  ,MonadReader (ClusterConfig e s t n,GCState t n) m
  ,MonadCodeBuilder e s t n m
  ,MonadCodeError e s t n m)
  => AnyCluster e s t n
  -> m (IOFiles e s)
clustToIoFiles =
  (>>= plansAndFiles)
  . traverseAnyRoles
  (fmap Input . matMaybe)
  (return . Intermediate)
  (fmap Output . matMaybe)
  . putVoidAny
  . first (const ())
  . (putIdentity :: AnyCluster e s t n
                 -> AnyCluster' (PlanSym e s) Identity (NodeRef t) (NodeRef n))

putVoidAny :: forall e n . AnyCluster' e Identity () (NodeRef n)
           -> AnyCluster' Void Identity () (NodeRef n)
putVoidAny = \case
  JoinClustW c -> JoinClustW $ dropIdentity
    $ WMetaD . first (const empty) . unMetaD <$> putIdentity c
  BinClustW c -> BinClustW $ dropIdentity
    $ WMetaD . first (const empty) . unMetaD <$> putIdentity c
  UnClustW c -> UnClustW $ dropIdentity
    $ WMetaD . first (const empty) . unMetaD <$> putIdentity c
  NClustW (NClust c) -> NClustW $ NClust c

splitNodeRoles :: Traversable f =>
                 Direction
               -> (a -> b, a -> b, a -> b)
               -> AnyCluster' e f t a
               -> AnyCluster' e f t b
splitNodeRoles dir (int,inp0,out0) =
  runIdentity . traverseAnyRoles (return . inp) (return . int) (return . out)
  where
    (inp,out) = case dir of
      ForwardTrigger -> (inp0, out0)
      ReverseTrigger -> (out0, inp0)

-- | Get the cluster corresponding to the node.
getClusterT :: (Hashables2 e s, HasCallStack,
               MonadState (ClusterConfig e s t n) m,
               MonadError (CodeBuildErr e s t n) m) =>
              NodeRef t
            -> m (CNFQuery e s, AnyCluster e s t n)
getClusterT t = maybe (throwError $ NoClusterForT t) return =<< lookupClusterT t

getClusterN :: (Hashables2 e s,
               MonadState (ClusterConfig e s t n) m,
               MonadError (CodeBuildErr e s t n) m) =>
              NodeRef n
            -> m [(CNFQuery e s, AnyCluster e s t n)]
getClusterN n = do
  metaSetM <- lookupClustersN n
  case metaSetM of
    [] -> throwError $ NoClusterForN n
    xs -> return $ fmap2 headErr $ toList xs
