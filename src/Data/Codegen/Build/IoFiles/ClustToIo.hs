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
{-# LANGUAGE ViewPatterns          #-}

module Data.Codegen.Build.IoFiles.ClustToIo
  (getClusterT
  ,toFiles
  ,clustToIoFiles) where

import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types
import           Data.Codegen.Build.IoFiles.MkFiles
import           Data.Codegen.Build.IoFiles.Types
import           Data.Codegen.Build.Monads.CodeBuilder
import           Data.Either
import           Data.Maybe
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.QuerySchema
import           Data.Query.SQL.FileSet
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL
import           Data.Utils.Tup
import           Data.Void
import           Prelude                               hiding (exp)

-- | Pattern match on the query and cluster that contains the
-- filepaths to generate code. (output, input)
toFiles :: IOFilesD e s -> Tup2 [Maybe (Maybe (QueryShape e s),FileSet)] -- out,in
toFiles IOFilesD{..} = maybeSwap $ splitIO $ toList iofCluster where
  maybeSwap (o,fmap3 DataFile -> i) = case iofDir of
    ForwardTrigger -> Tup2 o i
    ReverseTrigger -> Tup2 i o
  splitIO :: [NodeRole a b c] -> ([c], [b])
  splitIO = partitionEithers . toEithers where
    toEithers []                  = []
    toEithers (Input x:xs)        = Right x:toEithers xs
    toEithers (Output x:xs)       = Left x:toEithers xs
    toEithers (Intermediate _:xs) = toEithers xs

type CanPutIdLocal c op e =
  CanPutIdentity
    (c Identity (Compose ((,) (Maybe (op e))) Identity))
    (c Identity Identity)
    Identity
    (Compose ((,) (Maybe (op e))) Identity)

matMaybe :: MonadReader (ClusterConfig e s t n,GCState t n) m
         => [NodeRef n] -> NodeRef n
         -> m (Maybe (NodeRef n))
matMaybe mats ref = return $ if ref `elem` mats then Just ref else Nothing
-- dropReader (asks snd) $ getNodeStateReader ref <&> \case
-- Concrete _ Mat -> Just ref
-- Initial Mat    -> Just ref
-- _              -> Nothing

-- | Turn the ends of a cluster into (Plan,File) pairs. This checks
-- first if the at the edge is (to be) materialized and puts Nothing
-- if it isn't.
clustToIoFiles
  :: forall e s t n m .
  (Hashables2 e s
  ,MonadReader (ClusterConfig e s t n,GCState t n) m
  ,MonadCodeBuilder e s t n m
  ,MonadCodeError e s t n m)
  => Tup2 [NodeRef n]
  -> Direction
  -> AnyCluster e s t n
  -> m (IOFiles e s)
clustToIoFiles (Tup2 inps outs) dir =
  plansAndFiles
  <=< traverseAnyRoles
    (fmap inpSide . matMaybe inps)
    (return . Intermediate)
    (fmap outSide . matMaybe outs)
  . putVoidAny
  . first (const ())
  . (putIdentity :: AnyCluster e s t n
                 -> AnyCluster' (ShapeSym e s) Identity (NodeRef t) (NodeRef n))
  where
    (inpSide,outSide) = case dir of
      ForwardTrigger -> (Input,Output)
      ReverseTrigger -> (Output,Input)

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
getClusterT
  :: (Hashables2 e s
     ,MonadState (ClusterConfig e s t n) m
     ,MonadAShowErr e s err m)
  => NodeRef t
  -> m (QNFQuery e s,AnyCluster e s t n)
getClusterT t =
  maybe (throwAStr $ "No cluster for: " ++ ashow t) return =<< lookupClusterT t
