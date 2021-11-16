{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

module Data.Codegen.Build.IoFiles.MkFiles (plansAndFiles) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Data.Bitraversable
import           Data.Cluster.Propagators
import           Data.Cluster.Types
import           Data.Codegen.Build.IoFiles.Types
import           Data.Codegen.Build.Monads.Class
import           Data.Codegen.Build.Monads.CodeBuilder
import           Data.NodeContainers
import           Data.Query.SQL.QFile
import           Data.QueryPlan.Types
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL
import           Data.Utils.Tup
import           Data.Void

type MakeFilesConstr e s t n m =
  (Hashables2 e s,
   MonadCodeBuilder e s t n m,
   MonadError (CodeBuildErr e s t n) m,
   MonadReader (ClusterConfig e s t n, GCState t n) m)
type family ChClust c x where
  ChClust c (AnyCluster' e f t n) = c f (ComposedType c e f) t n
  ChClust c a = Void

type JIOFilesG a b c = ChClust JoinClust' (IOFilesG a b c)
type JIOFiles e s = ChClust JoinClust' (IOFiles e s)


-- | Insert file paths where necesary.
plansAndFiles
  :: forall e s t n m .
  MakeFilesConstr e s t n m
  => IOFilesG (NodeRef n) (NodeRef n) (NodeRef n)
  -> m (IOFiles e s)
plansAndFiles clst = case clst of
  JoinClustW jclust -> JoinClustW <$> makeFilesJUnsafe jclust
  clust             -> makeFilesNaive clust

-- | Populate the cluster edges with plans and filesets.
makeFilesNaive
  :: forall e s t n m .
  MakeFilesConstr e s t n m
  => IOFilesG (NodeRef n) (NodeRef n) (NodeRef n)
  -> m (IOFiles e s)
makeFilesNaive =
  traverse
  $ tritraverse
    (return . \(NodeRef n) -> NodeRef n)
    (fmap3 dataFilePath . getPlanAndQFile)
    getPlanAndQFile


populateNodeRole
  :: forall e s t n m .
  (MakeFilesConstr e s t n m)
  => NodeRole (NodeRef n) (Maybe (NodeRef n)) (Maybe (NodeRef n))
  -> m
    (NodeRole
       (NodeRef ())
       (Maybe (Maybe (QueryShape e s),FilePath))
       (Maybe (Maybe (QueryShape e s),QFile)))
populateNodeRole =
  fmap coerceI
  . bitraverse
  (fmap3 dataFilePath . getPlanAndQFile)
  getPlanAndQFile

coerceI :: NodeRole (NodeRef n) b c -> NodeRole (NodeRef ()) b c
coerceI = \case
  Input a                  -> Input a
  Output a                 -> Output a
  Intermediate (NodeRef i) -> Intermediate (NodeRef i)

-- | Lookup the plan and the fileset.
getPlanAndQFile
  :: forall e s t n m .
  (MakeFilesConstr e s t n m)
  => Maybe (NodeRef n)
  -> m (Maybe (Maybe (QueryShape e s),QFile))
getPlanAndQFile =
  traverse
  $ distrib
    (dropReader (asks fst) . getNodeShapeFull)
    (\r -> dropReader (asks fst) (getNodeFile r)
     >>= maybe (dropReader (asks fst) $ mkNodeFile r) return)

-- | Join complements have DataSet rather than single files on the
-- complements. This does not check the existance of input/output
-- nodefiles.
makeFilesJUnsafe
  :: forall e s t n m .
  MakeFilesConstr e s t n m
  => JIOFilesG (NodeRef n) (NodeRef n) (NodeRef n)
  -> m (JIOFiles e s)
makeFilesJUnsafe jclust = do
  Tup2 jclAntijoin jcrAntijoin <- traverse2 populateNodeRole
    $ Tup2 (joinClusterLeftAntijoin jclust) (joinClusterRightAntijoin jclust)
  Tup2 jclIntermediate jcrIntermediate <- traverse2 coerce'
    $ Tup2
      (joinClusterLeftIntermediate jclust)
      (joinClusterRightIntermediate jclust)
  jbClust <- makeFilesNaive (BinClustW $ joinBinCluster jclust) <&> \case
    BinClustW c -> c
    _           -> error "Unreachable"
  return
    $ updateCHash
      JoinClust
      { joinClusterLeftAntijoin = jclAntijoin
       ,joinClusterRightAntijoin = jcrAntijoin
       ,joinClusterLeftIntermediate = jclIntermediate
       ,joinClusterRightIntermediate = jcrIntermediate
       ,joinClusterLeftSplit = joinClusterLeftSplit jclust
       ,joinClusterRightSplit = joinClusterRightSplit jclust
       ,joinBinCluster = jbClust
       ,joinClusterHash = undefined
      }
  where
    coerce'
      :: NodeRole (NodeRef n) (Maybe (NodeRef n)) (Maybe (NodeRef n))
      -> m
        (NodeRole
           (NodeRef ())
           (Maybe (Maybe (QueryShape e s),FilePath))
           (Maybe (Maybe (QueryShape e s),QFile)))
    coerce' = \case
      Intermediate (NodeRef a) -> return $ Intermediate $ NodeRef a
      _                        -> error "oops non-interm role for interm node"

tritraverse :: Functor f =>
              (a -> f a')
            -> (b -> f b')
            -> (c -> f c')
            -> NodeRole a  b c
            -> f (NodeRole a' b' c')
tritraverse im i o = \case
  Input a        -> Input <$> i a
  Output a       -> Output <$> o a
  Intermediate a -> Intermediate <$> im a


distrib :: Applicative f => (a -> f x) -> (a -> f y) -> a -> f (x,y)
distrib f g a = (,) <$> f a <*> g a
