{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies     #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Data.QueryPlan.History (pastCosts,HCost) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Types
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.NodeContainers
import           Data.Pointed
import           Data.Proxy
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.Cert
import           Data.QueryPlan.Comp
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.HistBnd
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.PlanMech
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.ListT
import           Data.Utils.Nat


type HCost = Cert (Comp Cost)
instance PlanMech (PlanT t n Identity) (CostParams HistTag n) n where
  mcGetMech Proxy ref = gets $ refLU ref . gcHistMechMap
  mcPutMech Proxy ref me =
    modify $ \gc -> gc { gcHistMechMap = refInsert ref me $ gcHistMechMap gc }
  mcMkCost Proxy _ref cost = point $ toComp cost
  mcIsMatProc Proxy = isMatCost
  mcCompStackVal Proxy _ref = BndRes $ point $ point nonComp

-- | The expected cost of the next query.
pastCosts :: Monad m => NodeSet n -> ListT (PlanT t n m) (Maybe HCost)
pastCosts extraMat = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return $ take 5 qs
  wrapTrace ("histCost" <: q)
    $ lift
    $ getCostPlan @HistTag Proxy extraMat (CapVal maxCap) q

maxCap :: HistCap Cost
maxCap =
  HistCap { hcMatsEncountered = 1,hcValCap = MinInf,hcNonCompTolerance = 0.7 }

-- | The materialized node indeed has a cost. That cost is
-- calculated by imposing a scaling on the cost it would have if it
-- were not materialized. This opens the door to an explosion in
-- computation.
isMatCost :: forall t n . NodeRef n -> HistProc t n -> HistProc t n
isMatCost ref matCost0 = wrapMealy matCost0 guardedGo
  where
    go conf matCost = do
      "Mat historical" <<: ref
      let conf0 = mapCap unscaleCap conf
      -- The cap used to run the arror will not match the scaled
      -- result. If that happens the outside process will fail to
      -- proceed and will keep asking for a larger result.
      (nxt,r) <- lift $ toKleisli (runMealyArrow matCost) conf0
      -- "result" <<: (ref,confCap conf,confCap conf0,r)
      conf' <- yieldMB $ case r of
        BndBnd bnd -> BndBnd $ incrementCert $ scaleMin bnd
        BndRes res -> BndRes $ scaleSum res
        _e         -> BndRes $ point $ point $ Comp 0.5 zero -- error is more highly non-computable but it's fre
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

-- | we may be overshooting here and there but it's ok
double :: Int -> Int
double i = if i < 0 then i else i * 2
halfCeil :: Int -> Int
halfCeil i =
  if
    | i <= 0    -> i
    | m > 0     -> d + 1
    | otherwise -> d
  where
    (d,m) = divMod i 2
scaleCost :: Cost -> Cost
scaleCost (Cost r w) = Cost (halfCeil r) (halfCeil w)
mapCap :: (ZCap w -> ZCap w) -> Conf w -> Conf w
mapCap f conf = conf { confCap = f <$> confCap conf }
unscaleCap :: HistCap Cost -> HistCap Cost
unscaleCap hc =
  hc { hcValCap = Min $ fmap unscaleCost $ getMin $ hcValCap hc
      ,hcMatsEncountered = 1 + hcMatsEncountered hc
     }
  where
    unscaleCost (Cost r w) = Cost (double r) (double w)

scaleMin :: Min HCost -> Min HCost
scaleMin m@(Min Nothing)         = m
scaleMin (Min (Just (Cert c (Comp d i)))) =
  Min $ Just $ Cert c $ Comp (matComputability d) $ scaleCost i
scaleSum :: Sum HCost -> Sum HCost
scaleSum m@(Sum Nothing)         = m
scaleSum (Sum (Just (Cert c (Comp d i)))) =
  Sum $ Just $ Cert c $ Comp (matComputability d) $ scaleCost i
