{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies     #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Data.QueryPlan.History
  (pastCosts) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import           Control.Arrow
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
--    ,mcCompStack = \_ref -> BndRes $ pure nonComp
--   }
--   where
--     mkCost _ref cost = toComp cost
--     histMapLens = Lens { getL = gcHistMechMap,modL = \f gc
--       -> gc { gcHistMechMap = f $ gcHistMechMap gc } }
instance PlanMech HistTag n where
  type PlanMechVal HistTag n = Comp Cost
  mcMechMapLens =
    Lens { getL = gcHistMechMap
          ,modL = (\f gc -> gc { gcHistMechMap = f $ gcHistMechMap gc })
         }
  mcMkCost Proxy _ref cost = toComp cost
  mcIsMatProc = noMatCost
  mcCompStack _ref = BndRes $ point nonComp

instance HasLens (Min (Comp Cost)) (Min (Comp Cost))
-- | The expected cost of the next query.
pastCosts :: Monad m => NodeSet n -> ListT (PlanT t n m) (Maybe PCost)
pastCosts extraMat = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return qs
  lift $ getCost @HistTag Proxy extraMat ForceResult q

-- | noMatCost is the cost of a non-materialized node. Each time it is
-- calculated it imposes a multiplication cap. As this means an
-- exponential effect on the signifiance of a term after a few
-- iterations it is not worth evaluating the actual cost. However we
-- want the actual cap passed over to keep rising to reflect the
-- invariants.
noMatCost
  :: NodeRef n
  -> HistProc t n
  -> HistProc t n
noMatCost _ref matCost = recur
  where
    recur = MealyArrow $ fromKleisli $ \conf -> do
      curScale :: Double <- undefined
      if curScale < 0.1 then return (recur,BndRes zero) else do
        (nxt,ret) <- id -- local (* factor)
          $ second changeBound
          <$> toKleisli (runMealyArrow matCost) (changeCap conf)
        return (nxt,case ret of
          BndErr _ -> BndRes zero
          _        -> ret)
    factor = 0.5
    changeBound = \case
      BndBnd b     -> BndBnd $ minMap (scalePCost factor) b
      BndRes r     -> BndRes $ sumMap (scalePCost factor) r
      e@(BndErr _) -> e
    changeCap conf = conf { confCap = case confCap conf of
      CapVal c    -> CapVal $ modL defLens (minMap (scalePCost (1 / factor))) c
      ForceResult -> ForceResult }




scalePCost = fmap . scaleCost
scaleCost :: Double -> Cost -> Cost
scaleCost sc Cost {..} =
  Cost { costReads = scaleI costReads,costWrites = scaleI costWrites }
  where
    scaleI x = round $ sc * fromIntegral x
