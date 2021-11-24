{-# LANGUAGE TypeApplications #-}
module Data.QueryPlan.Matable
  (isMaterializable,isMaterializableSlow) where
import           Control.Antisthenis.Bool
import           Control.Antisthenis.Types
import           Control.Monad.Extra
import           Control.Monad.State
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


isMaterializableSlow'
  :: forall t n m .
  Monad m
  => Bool
  -> [NodeRef n]
  -> NodeRef n
  -> PlanT t n m Bool
isMaterializableSlow' countProt dels =
  (`evalStateT` mempty) . go
  where
    go ref = gets (refLU ref) >>= \case
      Just v -> return v
      Nothing -> do
        lift (getNodeState ref) >>= \case
          Concrete _ NoMat -> checkMats >>= cacheAndRet
          Concrete _ Mat   -> cacheAndRet $ ref `notElem` dels
          Initial NoMat    -> checkMats >>= cacheAndRet
          Initial Mat      -> cacheAndRet $ ref `notElem` dels
      where
        cacheAndRet res = do
          modify $ refInsert ref res
          return res
        checkMats = do
          neigh <- lift
            $ fmap2 (toNodeList . metaOpIn . fst)
            $ findCostedMetaOps ref
          modify $ refInsert ref False
          anyM (allM go) neigh

isMaterializableSlow
  :: forall t n m .
  Monad m
  => Bool
  -> [NodeRef n]
  -> NodeRef n
  -> PlanT t n m Bool
isMaterializableSlow _coutProt dels =
  (`evalStateT` mempty) . go
  where
    go ref = gets (refLU ref) >>= \case
      Just v -> return v
      Nothing -> do
        ism <- lift $ isMat <$> getNodeState ref
        res <- if ism && ref `notElem` dels then return True else checkMats
        modify $ refInsert ref res
        return res
      where
        checkMats = do
          neigh <- lift
            $ fmap2 (toNodeList . metaOpIn . fst)
            $ findCostedMetaOps ref
          modify $ refInsert ref False
          anyM (allM go) neigh
