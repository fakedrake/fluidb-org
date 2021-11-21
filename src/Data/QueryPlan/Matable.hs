{-# LANGUAGE TypeApplications #-}
module Data.QueryPlan.Matable
  (isMaterializable,isMaterializableSlow) where
import           Control.Antisthenis.Bool
import           Control.Antisthenis.Types
import           Control.Monad.Extra
import           Data.NodeContainers
import           Data.Proxy
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.Functors

isMaterializable
  :: forall t n m . Monad m => [NodeRef n] -> NodeRef n -> PlanT t n m Bool
isMaterializable noMats ref = do
  res <- getPlanBndR
    @(MatParams n)
    Proxy
    (fromRefAssocs $ (,False) <$> noMats)
    ForceResult
    ref
  case res of
    BndErr _e -> throwPlan "Antisthenis should have figured errors are False."
    BndRes (BoolV r) -> return r
    BndBnd _b -> throwPlan "Tried to force result but got bound"


isMaterializableSlow
  :: forall t n m . Monad m => [NodeRef n] -> NodeRef n -> PlanT t n m Bool
isMaterializableSlow
  dels = wrapTrace ("isMaterializableSlow" ++ show ref) $ go ref
  where
    go ref = do
      ism <- isMat <$> getNodeState ref
      if ism && ref `notElem` dels then return True else checkMats ref
    checkMats ref = do
      neigh :: [[NodeRef n]] <- fmap2 (toNodeList . metaOpIn . fst)
        $ findCostedMetaOps ref
      anyM allMat neigh
    allMat refs = allM go refs
