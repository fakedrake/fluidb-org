{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

module Data.Cluster.PutCluster
  (putBinCluster
  ,putUnCluster
  ,putNCluster
  ,idempotentClusterInsert
  ,shapeSymAssoc) where

import           Control.Applicative
import           Control.Monad
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Propagators
import           Data.Cluster.PutCluster.Common
import           Data.Cluster.Types
import           Data.Cluster.Types.Monad
import           Data.CppAst.CppType
import           Data.NodeContainers
import           Data.QnfQuery.BuildUtils
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Utils.Debug
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.Tup


-- | Create the T node and connect it to the inputs
putBinCluster
  :: (Hashables2 e s,Monad m)
  => [(ShapeSym e s,ShapeSym e s)]
  -> BQOp (ShapeSym e s)
  -> (NodeRef n,NQNFQuery e s) -- inL
  -> (NodeRef n,NQNFQuery e s) -- inR
  -> (NodeRef n,NQNFQuery e s) -- Out
  -> CGraphBuilderT e s t n m (BinClust e s t n)
putBinCluster symAssoc op (l,_nqnfL) (r,_nqnfR) (out,nqnfO) = do
  clust <- idempotentClusterInsert constraints mkClust
  putBClustPropagator clust symAssoc op
  forM_ [nqnfO] $ \nqnf -> linkQnfClust (nqnfToQnf nqnf) $ BinClustW clust
  return clust
  where
    constraints =
      [(binClusterLeftIn,l),(binClusterRightIn,r),(binClusterOut,out)]
    mkClust = do
      let qnfO = nqnfToQnf nqnfO
      tRef <- mkNodeFromQnfT qnfO
      let rev = getReversibleB op
      let linkTo nref =
            linkNodes
              NodeLinkDescr
              { nldNSide = Out,nldIsRev = rev,nldTNode = tRef,nldNNode = nref }
      let linkFrom nref =
            linkNodes
              NodeLinkDescr
              { nldNSide = Inp,nldIsRev = rev,nldTNode = tRef,nldNNode = nref }
      lift2 $ do
        linkTo out
        mapM_ linkFrom [l,r]
      let clust =
            updateCHash
              BinClust
              { binClusterLeftIn = noopRef l
               ,binClusterRightIn = noopRef r
               ,binClusterOut = withOpRef op out
               ,binClusterT = tRef
               ,binClusterHash = undefined
              }
      forM_ [l,r] $ \ref -> registerClusterInput ref $ BinClustW clust
      return clust

-- PUTPROP
putBClustPropagator
  :: (Hashables2 e s,Monad m)
  => BinClust e s t n
  -> [(ShapeSym e s,ShapeSym e s)]
  -> BQOp (ShapeSym e s)
  -> CGraphBuilderT e s t n m ()
putBClustPropagator clust assoc op =
  putShapePropagator (BinClustW clust)
  $ ACPropagatorAssoc
  { acpaPropagator = cPropToACProp $ binClustPropagator assoc op
   ,acpaInOutAssoc = assoc
  }

shapeSymAssoc :: Hashables2 e s => NQNFResultDF d f a e s -> [(ShapeSym e s,ShapeSym e s)]
shapeSymAssoc = fmap (bimap mkSym mkSym) . nqnfResInOutNames where
  mkSym (e,col) = mkShapeSym (Column col 0) e

-- | Connect 3 nodes with a unary cluster. Note that it is possible
-- for clusters to have coinciding input/output eg it is possible
-- (QProd .. A == A).
putUnCluster
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => ([(ShapeSym e s,ShapeSym e s)],[(ShapeSym e s,ShapeSym e s)])
  -- ^ Only the primary is required
  -> (e -> Maybe CppType)
  -> (UQOp (ShapeSym e s),Maybe (UQOp (ShapeSym e s)))
  -> (NodeRef n,NQNFQuery e s)
  -> (NodeRef n,NQNFQuery e s)
  -> (NodeRef n,NQNFQuery e s)
  -> CGraphBuilderT e s t n m (UnClust e s t n)
putUnCluster
  (symAssocPrim,symAssocSec)
  literalType
  (op,coopM)
  (inp,_nqnfI)
  (secRef,nqnfCoO)
  (refO,nqnfO) = do
  c <- idempotentClusterInsert constraints mkClust
  putUnClustPropagator (Tup2 symAssocPrim symAssocSec) literalType c op
  forM_ [nqnfO,nqnfCoO] $ \nqnf
    -> linkQnfClust (nqnfToQnf nqnf) $ UnClustW c
  return c
  where
    constraints =
      [(unClusterIn,inp)
      ,(unClusterSecondaryOut,secRef)
      ,(unClusterPrimaryOut,refO)]
    mkClust = do
      let qnfO = nqnfToQnf nqnfO
       -- The same is clustered.
      tRef <- mkNodeFromQnfT qnfO
      let rev = getReversibleU op
      let linkTo nref =
            linkNodes
              NodeLinkDescr
              { nldNSide = Out,nldIsRev = rev,nldTNode = tRef,nldNNode = nref }
      let linkFrom nref =
            linkNodes
              NodeLinkDescr
              { nldNSide = Inp,nldIsRev = rev,nldTNode = tRef,nldNNode = nref }
      lift2 $ do
        mapM_ linkTo [secRef,refO]
        linkFrom inp
      let clust :: UnClust e s t n =
            updateCHash
              UnClust
              { unClusterIn = noopRef inp
               ,unClusterPrimaryOut = withOpRef op refO
               ,unClusterSecondaryOut =
                  withOpRefM (maybe empty pure coopM) secRef
               ,unClusterT = tRef
               ,unClusterHash = undefined
              }
      registerClusterInput inp $ UnClustW clust
      return clust

putUnClustPropagator
  :: (Hashables2 e s,Monad m)
  => Tup2 [(ShapeSym e s,ShapeSym e s)]
  -> (e -> Maybe CppType)
  -> UnClust e s t n
  -> UQOp (ShapeSym e s)
  -> CGraphBuilderT e s t n m ()
putUnClustPropagator symAssocs literalType clust op =
  putShapePropagator
    (UnClustW clust)
    ACPropagatorAssoc
    { acpaPropagator =
        cPropToACProp $ unClustPropagator symAssocs literalType op
     ,acpaInOutAssoc = concat symAssocs
    }

putNCluster
  :: (Hashables2 e s,Monad m)
  => QueryShape e s
  -> (NodeRef n,NQNFQuery e s)
  -> CGraphBuilderT e s t n m (NClust e s t n)
putNCluster shape (ref,nqnf) = idempotentClusterInsert [(nClustNode,ref)] $ do
  linkQnfClust (nqnfToQnf nqnf) $ NClustW $ NClust ref
  putShapePropagator (NClustW $ NClust ref)
    $ ACPropagatorAssoc
    { acpaPropagator = cPropToACPropN $ nClustPropagator shape
     ,acpaInOutAssoc = []
    }
  return $ NClust ref
