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
import           Data.QueryPlan.Cert
import           Data.QueryPlan.Comp
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.HistBnd
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.ListT
import           Data.Utils.Nat


type HCost = Cert (Comp Cost)
instance PlanMech HistTag n where
  mcMechMapLens = Lens { getL = gcHistMechMap,modL = \f gc ->
    gc { gcHistMechMap = f $ gcHistMechMap gc } }
  mcMkCost Proxy _ref cost = point $ toComp cost
  mcIsMatProc = isMatCost
  mcCompStackVal _ref = BndRes $ point $ point nonComp

-- | The expected cost of the next query.
pastCosts :: Monad m => NodeSet n -> ListT (PlanT t n m) (Maybe HCost)
pastCosts extraMat = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return qs
  lift
    $ wrapTrace ("pastCosts " ++ ashow q)
    $ getCost @HistTag Proxy extraMat ForceResult q

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
        BndBnd bnd -> BndBnd $ incrementCert $ scaleMin bnd
        BndRes res -> BndRes $ scaleSum res
        _e         -> BndRes $ point $ point $ Comp 0.5 zero -- error is more highly non-computable but it's free
      return (conf',nxt)
    guardedGo conf matCost = if isTooDeep conf then do
      conf' <- yieldMB $ BndRes zero
      return (conf',matCost) else go conf matCost
    isTooDeep c = case confCap c of
      CapVal cap  -> hcMatsEncountered cap > 3
      ForceResult -> False

-- Nothe that the error is thoun automatically if it is not
-- computable.

incrementCert :: Min HCost -> Min HCost
incrementCert (Min (Just (Cert c (Comp d i)))) =
  Min (Just (Cert (c + 1) (Comp d i)))
incrementCert a = a
matComputability :: Double -> Double
matComputability d = 1 - 0.8 * (1 - d)
scaleCost :: Cost -> Cost
scaleCost (Cost r w) = Cost (r `div` 2) (w `div` 2)

scaleMin :: Min HCost -> Min HCost
scaleMin m@(Min Nothing)         = m
scaleMin (Min (Just (Cert c (Comp d  i)))) =
  Min $ Just $ Cert c $ Comp (matComputability d) $ scaleCost i
scaleSum :: Sum HCost -> Sum HCost
scaleSum m@(Sum Nothing)         = m
scaleSum (Sum (Just (Cert c (Comp d i)))) =
  Sum $ Just $ Cert c $ Comp (matComputability d) $ scaleCost i
