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
import           Data.Utils.Debug
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
isMaterializableSlow dels ref0 =
  wrapTrace ("isMaterializableSlow" ++ show ref0) $ go mempty ref0
  where
    go cache ref = case ref `refLU` cache of
      Just v -> return v
      Nothing -> do
        ism <- isMat <$> getNodeState ref
        if ism && ref `notElem` dels then return True else checkMats
        where
          checkMats = do
            neigh :: [[NodeRef n]] <- fmap2 (toNodeList . metaOpIn . fst)
              $ findCostedMetaOps ref
            anyM allMat neigh
          allMat refs = allM (go $ refInsert ref False cache) refs
