{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE TypeFamilies      #-}
module Data.QueryPlan.PlanMech (PlanMech(..),MetaTag) where

import           Control.Antisthenis.Types
import           Control.Arrow                   hiding (first)
import           Control.Monad.Identity
import           Control.Monad.State
import           Data.Bifunctor
import           Data.NodeContainers
import           Data.Proxy
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Types
import           Data.Utils.Functors
import           Data.Utils.Nat

class Monad m => PlanMech m tag n where
  mcGetNeighbors :: Proxy tag -> NodeRef n -> m [([NodeRef n],Cost)]
  default mcGetNeighbors
    :: (m ~ PlanT t n Identity)
    => Proxy tag
    -> NodeRef n
    -> m [([NodeRef n],Cost)]
  mcGetNeighbors Proxy ref =
    fmap2 (first $ toNodeList . metaOpIn) $ findCostedMetaOps ref

  mcPutMech :: Proxy tag -> NodeRef n -> ArrProc tag m -> m ()
  mcGetMech :: Proxy tag -> NodeRef n -> m (Maybe (ArrProc tag m))
  mcMkCost :: Proxy (m (),tag) -> NodeRef n -> Cost -> MechVal (MetaTag tag)

  -- Modify cost machine if the ref is materialized.
  mcIsMatProc
    :: Proxy (m (),tag) -> NodeRef n -> ArrProc tag m -> ArrProc tag m
  mcCompStackVal :: Proxy (m ()) -> NodeRef n -> BndR tag

-- | Make a plan for a node to be concrete.
instance PlanMech (PlanT t n Identity) (CostParams CostTag n) n where
  mcIsMatProc Proxy _ref _proc = arr $ const $ BndRes zero
  mcGetMech Proxy ref = gets $ refLU ref . gcMechMap
  mcPutMech Proxy ref m =
    modify $ \gsc -> gsc { gcMechMap = refInsert ref m $ gcMechMap gsc }
  mcMkCost Proxy ref cost =
    PlanCost { pcPlan = Just $ nsSingleton ref,pcCost = cost }
  mcCompStackVal _ n = BndErr $ ErrCycleEphemeral n
