{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeFamilies      #-}
module Data.QueryPlan.PlanMech (PlanMech(..),MetaTag) where

import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Bool
import           Control.Antisthenis.Convert
import           Control.Antisthenis.Types
import           Control.Antisthenis.Zipper
import           Control.Antisthenis.ZipperId
import           Control.Arrow                              hiding (first)
import           Control.Monad.Identity
import           Control.Monad.State
import           Data.Bifunctor
import           Data.NodeContainers
import           Data.Proxy
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Nat



-- | depset has a constant part that is the cost of triggering and a
-- variable part.
data DSetR v p = DSetR { dsetConst :: Sum v,dsetNeigh :: [p] }
  deriving (Functor,Foldable,Traversable)

-- | The dependency set in terms of processes.
type DSet m tag v = DSetR v (ArrProc tag m)

makeCostProc
  :: forall n tag m .
  (Monad m,IsPlanCostParams (CostParams tag n) n)
  => NodeRef n
  -> [DSet m (CostParams tag n) (MechVal (PlanParams tag n))]
  -> ArrProc (CostParams tag n) m
makeCostProc ref deps =
  convArrProc convMinSum
  $ mkProcId (zidDefault $ "min" <: ref)
  $ zipWith go [1::Int ..] deps
  where
    go i DSetR {..} = convArrProc convSumMin $ p $ constArr : dsetNeigh
      where
        p = mkProcId $ zidDefault $ "sum[" ++ show i ++ "]" <: ref
        constArr = arr $ const $ BndRes dsetConst

makeMatProc
  :: forall n m .
  (Monad m,HasCallStack)
  => NodeRef n
  -> [[ArrProc (MatParams n) m]]
  -> ArrProc (MatParams n) m
makeMatProc ref deps = mkOrProc $ mkAndProc <$> deps
  where
    mkAndProc :: [ArrProc (MatParams n) m]
              -> ArrProc (BoolTag And (PlanParams (MatTag0 And) n)) m
    mkAndProc =
      mkProcId (zidDefault $ "and" <: ref)
      . fmap (convArrProc $ coerceConv GenericConv)
    mkOrProc :: [ArrProc (BoolTag And (PlanParams (MatTag0 And) n)) m]
             -> ArrProc (BoolTag Or (PlanParams MatTag n)) m
    mkOrProc =
      mkProcId (zidDefault $ "or" <: ref)
      . fmap (convArrProc $ coerceConv GenericConv)

class Monad m => PlanMech m w n where
  mcPutMech :: Proxy w -> NodeRef n -> ArrProc w m -> m ()
  mcGetMech :: Proxy w -> NodeRef n -> m (Maybe (ArrProc w m))
  mcMkCost :: Proxy (m (),w) -> NodeRef n -> Cost -> MechVal (MetaTag w)

  -- Modify cost machine if the ref is materialized.
  mcIsMatProc :: Proxy (m (),w) -> NodeRef n -> ArrProc w m -> ArrProc w m
  mcCompStackVal :: Proxy (m ()) -> NodeRef n -> BndR w

  -- | Check if a value counts as being computable. In most cases
  -- non-comp is an error but for historical nodes it is if it's
  -- uncertainty is > 0.7.
  mcIsComputable :: Proxy (m (),w,n) -> BndR w -> Bool
  default mcIsComputable :: Proxy (m (),w,n) -> BndR w -> Bool
  mcIsComputable _ = \case
    BndErr _ -> False
    _        -> True

  mcMkProcess :: (NodeRef n -> ArrProc w m) -> NodeRef n -> ArrProc w m
  default mcMkProcess
    :: (m ~ PlanT t n Identity
       ,w ~ CostParams tag n
       ,IsPlanCostParams (CostParams tag n) n)
    => (NodeRef n -> ArrProc w m)
    -> NodeRef n
    -> ArrProc w m
  mcMkProcess getOrMakeMech ref = squashMealy $ \conf -> do
    -- mops <- lift $ fmap2 (first $ toNodeList . metaOpIn) $ findCostedMetaOps ref
    neigh
      <- lift $ fmap2 (first $ toNodeList . metaOpIn) $ findCostedMetaOps ref
    -- ("metaops(" ++ ashow ref ++ ")") <<: mops
    let mechs =
          [DSetR { dsetConst = Sum $ Just $ mcMkCost @m @w Proxy ref cost
                  ,dsetNeigh = [getOrMakeMech n | n <- inp]
                 } | (inp,cost) <- neigh]
    return (conf,makeCostProc ref mechs)

-- | Make a plan for a node to be concrete.
instance PlanMech (PlanT t n Identity) (CostParams CostTag n) n where
  mcIsMatProc Proxy _ref _proc = arr $ const $ BndRes zero
  mcGetMech Proxy ref = gets $ refLU ref . gcMechMap
  mcPutMech Proxy ref m =
    modify $ \gsc -> gsc { gcMechMap = refInsert ref m $ gcMechMap gsc }
  mcMkCost Proxy ref cost =
    PlanCost { pcPlan = Just $ nsSingleton ref,pcCost = cost }
  mcCompStackVal _ n = BndErr $ ErrCycleEphemeral n


nodeStatePair :: Monad m => NodeRef n -> PlanT t n m (NodeState, Bool)
nodeStatePair = fmap (\x -> (x,isMat x)) . getNodeState
instance PlanMech (PlanT t n Identity) (MatParams n) n where
  mcGetMech Proxy ref = do
    x <- nodeStatePair ref
    trM $ "Lu" <: x
    gets $ refLU ref . matableMechMap
  mcPutMech Proxy ref m = modify $ \gsc ->
    gsc { matableMechMap = refInsert ref m $ matableMechMap gsc }
  mcIsMatProc Proxy _ref _proc = arr $ const $ BndRes $ BoolV True
  -- | The NodeProc system registers the cycle as noncomp in the
  -- coepoch.
  mcMkCost = error "We dont' make costs for Bool (materializable) mech"
  mcCompStackVal _ _n = BndRes $ BoolV False
  mcMkProcess getOrMakeMech ref = squashMealy $ \conf -> lift $ do
    neigh :: [[NodeRef n]] <- fmap2 (toNodeList . metaOpIn . fst)
      $ findCostedMetaOps ref
    neigh' <- traverse2 nodeStatePair neigh
    trM $ "Neighbors: " ++ show (ref,neigh')
    -- If the node is materialized const true, otherwise const
    -- false. The coepochs are updated by the caller, particularly by
    -- ifMaterialized. Since we got here here it means ref is not
    -- materialized.
    return
      $ (conf,)
      $ if null neigh then arrConst $ BndRes $ BoolV False
      else makeMatProc ref $ [getOrMakeMech <$> inps | inps <- neigh]
    where
      arrConst = arr . const
