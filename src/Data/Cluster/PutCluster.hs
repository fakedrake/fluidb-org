{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

module Data.Cluster.PutCluster
  ( putBinCluster
  , putUnCluster
  , putNCluster
  , idempotentClusterInsert
  , planSymAssoc
  ) where

import           Control.Applicative
import           Control.Monad
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Propagators
import           Data.Cluster.PutCluster.Common
import           Data.Cluster.Types
import           Data.CnfQuery.BuildUtils
import           Data.CnfQuery.Types
import           Data.CppAst.CppType
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Utils.Debug
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.Tup


-- | Create the T node and connect it to the inputs
putBinCluster
  :: (Hashables2 e s,Monad m)
  => [(PlanSym e s,PlanSym e s)]
  -> BQOp (PlanSym e s)
  -> (NodeRef n,NCNFQuery e s) -- inL
  -> (NodeRef n,NCNFQuery e s) -- inR
  -> (NodeRef n,NCNFQuery e s) -- Out
  -> CGraphBuilderT e s t n m (BinClust e s t n)
putBinCluster symAssoc op (l,_ncnfL) (r,_ncnfR) (out,ncnfO) = do
  traceM "putBinCluster"
  clust <- idempotentClusterInsert constraints mkClust
  putBClustPropagator clust symAssoc op
  forM_ [ncnfO] $ \ncnf -> linkCnfClust (ncnfToCnf ncnf) $ BinClustW clust
  return clust
  where
    constraints =
      [(binClusterLeftIn,l),(binClusterRightIn,r),(binClusterOut,out)]
    mkClust = do
      let cnfO = ncnfToCnf ncnfO
      tRef <- mkNodeFromCnfT cnfO
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
putBClustPropagator :: (Hashables2 e s, Monad m) =>
                      BinClust e s t n
                    -> [(PlanSym e s,PlanSym e s)]
                    -> BQOp (PlanSym e s)
                    -> CGraphBuilderT e s t n m ()
putBClustPropagator clust assoc op = putPlanPropagator
  (BinClustW clust)
  (cPropToACProp $ binClustPropagator assoc op, assoc)

planSymAssoc :: Hashables2 e s => NCNFResultDF d f a e s -> [(PlanSym e s,PlanSym e s)]
planSymAssoc = fmap (bimap mkSym mkSym) . ncnfResInOutNames where
  mkSym (e,col) = mkPlanSym (Column col 0) e

-- | Connect 3 nodes with a unary cluster. Note that it is possible
-- for clusters to have coinciding input/output eg it is possible
-- (QProd .. A == A).
putUnCluster
  :: forall e s t n m .
  (HasCallStack,Hashables2 e s,Monad m)
  => ([(PlanSym e s,PlanSym e s)],[(PlanSym e s,PlanSym e s)])
  -- ^ Only the primary is required
  -> (e -> Maybe CppType)
  -> (UQOp (PlanSym e s),Maybe (UQOp (PlanSym e s)))
  -> (NodeRef n,NCNFQuery e s)
  -> (NodeRef n,NCNFQuery e s)
  -> (NodeRef n,NCNFQuery e s)
  -> CGraphBuilderT e s t n m (UnClust e s t n)
putUnCluster
  (symAssocPrim,symAssocSec)
  literalType
  (op,coopM)
  (inp,_ncnfI)
  (secRef,ncnfCoO)
  (refO,ncnfO) = do
  traceM "putUnCluster"
  c <- idempotentClusterInsert constraints mkClust
  putUnClustPropagator (Tup2 symAssocPrim symAssocSec) literalType c op
  forM_ [ncnfO,ncnfCoO] $ \ncnf
    -> linkCnfClust (ncnfToCnf ncnf) $ UnClustW c
  return c
  where
    constraints = [(unClusterIn,inp),(unClusterPrimaryOut,refO)]
    mkClust = do
      let cnfO = ncnfToCnf ncnfO
       -- The same is clustered.
      tRef <- mkNodeFromCnfT cnfO
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
  :: (HasCallStack,Hashables2 e s,Monad m)
  => Tup2 [(PlanSym e s,PlanSym e s)]
  -> (e -> Maybe CppType)
  -> UnClust e s t n
  -> UQOp (PlanSym e s)
  -> CGraphBuilderT e s t n m ()
putUnClustPropagator symAssocs literalType clust op =
  putPlanPropagator (UnClustW clust)
  (cPropToACProp $ unClustPropagator symAssocs literalType op,
   concat symAssocs)

putNCluster
  :: (Hashables2 e s,Monad m)
  => QueryPlan e s
  -> (NodeRef n,NCNFQuery e s)
  -> CGraphBuilderT e s t n m (NClust e s t n)
putNCluster plan (ref,ncnf) = idempotentClusterInsert [(nClustNode,ref)] $ do
  linkCnfClust (ncnfToCnf ncnf) $ NClustW $ NClust ref
  putPlanPropagator
    (NClustW $ NClust ref)
    (cPropToACPropN $ nClustPropagator plan,[])
  return $ NClust ref
