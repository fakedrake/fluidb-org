{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies     #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Data.QueryPlan.History
  (pastCosts) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Types
import           Control.Monad.Reader
import           Data.NodeContainers
import           Data.Pointed
import           Data.Proxy
import           Data.QueryPlan.Comp
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.ListT
import           Data.Utils.Nat

-- historicalCostConf :: MechConf t n PCost
-- historicalCostConf =
--   MechConf
--   { mcMechMapLens = histMapLens
--    ,mcMkCost = mkCost
--    ,mcIsMatProc = noMatCost
--    ,mcCompStackVal = \_ref -> BndRes $ pure nonComp
--   }
--   where
--     mkCost _ref cost = toComp cost
--     histMapLens = Lens { getL = gcHistMechMap,modL = \f gc
--       -> gc { gcHistMechMap = f $ gcHistMechMap gc } }
instance PlanMech HistTag n where
  type PlanMechVal HistTag n = Comp Cost
  mcMechMapLens = Lens { getL = gcHistMechMap,modL = \f gc ->
    gc { gcHistMechMap = f $ gcHistMechMap gc } }
  mcMkCost Proxy _ref cost = toComp cost
  mcIsMatProc = isMatCost
  mcCompStackVal _ref = BndRes $ point nonComp

instance HasLens (Min (Comp Cost)) (Min (Comp Cost))
-- | The expected cost of the next query.
pastCosts :: Monad m => NodeSet n -> ListT (PlanT t n m) (Maybe PCost)
pastCosts extraMat = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return qs
  lift $ getCost @HistTag Proxy extraMat ForceResult q

incrMats :: Conf (CostParams HistTag n)
         -> Conf (CostParams HistTag n)
incrMats conf =
  conf { confCap = case confCap conf of
    ForceResult -> CapVal HistCap { hcMatsEncountered = 1,hcValCap = MinInf }
    CapVal
      cap -> CapVal $ cap { hcMatsEncountered = 1 + hcMatsEncountered cap } }

-- | The materialized node indeed has a cost. That cost is
-- calculated by imposing a scaling on the cost it would have if it
-- were not materialized. This opens the door to an explosion in
-- computation.
isMatCost :: forall t n . NodeRef n -> HistProc t n -> HistProc t n
isMatCost _ref matCost0 = wrapMealy matCost0 guardedGo
  where
    go conf matCost = do
      (nxt,r) <- lift $ toKleisli (runMealyArrow matCost) $ incrMats conf
      conf' <- yieldMB $ case r of
        BndBnd bnd -> BndBnd $ scaleMin bnd
        BndRes res -> BndRes $ scaleSum res
        e          -> BndRes $ point $ Comp 0.5 zero -- error is more highly non-computable but it's free
      return (conf',nxt)
    guardedGo conf matCost = if isTooDeep conf then do
      conf' <- yieldMB $ BndRes zero
      return (conf',matCost) else go conf matCost
    isTooDeep c = case confCap c of
      CapVal cap  -> hcMatsEncountered cap > 3
      ForceResult -> False

-- XXX: Throw an error if it's not computable:
-- * The only sum increases computability.

matComputability :: Double -> Double
matComputability d = 1 - 0.8 * (1 - d)
matCost :: Cost -> Cost
matCost (Cost r w) = Cost (r `div` 2) (w `div` 2)

scaleMin :: Min (Comp Cost) -> Min (Comp Cost)
scaleMin m@(Min Nothing)         = m
scaleMin (Min (Just (Comp d i))) =
  Min $ Just $ Comp (matComputability d) (matCost i)
scaleSum :: Sum (Comp Cost) -> Sum (Comp Cost)
scaleSum m@(Sum Nothing)         = m
scaleSum (Sum (Just (Comp d i))) =
  Sum $ Just $ Comp (matComputability d) (matCost i)
