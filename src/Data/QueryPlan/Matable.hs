{-# LANGUAGE TypeApplications #-}
module Data.QueryPlan.Matable
  (isMaterializable) where
import           Control.Antisthenis.Bool
import           Control.Antisthenis.Types
import           Data.NodeContainers
import           Data.Proxy
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Types
import           Data.Utils.Debug

isMaterializable :: forall t n m . Monad m => NodeRef n -> PlanT t n m Bool
isMaterializable ref = wrapTraceT ("antisthenis:isMaterializable " ++ show ref) $ do
  res <- getPlanBndR @(MatParams n) Proxy ForceResult ref
  case res of
    BndErr _e -> throwPlan "Antisthenis should have figured errors are False."
    BndRes (BoolV r) -> return r
    BndBnd _b -> throwPlan "Tried to force result but got bound"
