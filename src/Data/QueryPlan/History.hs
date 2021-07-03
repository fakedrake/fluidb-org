module Data.QueryPlan.History
  (pastCosts) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import           Control.Arrow
import           Control.Monad.Reader
import           Data.NodeContainers
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.ListT
import           Data.Utils.Monoid

-- | The expected cost of the next query.
pastCosts :: Monad m => ListT (PlanT t n m) (Maybe Cost)
pastCosts = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return qs
  lift $ getCost noMatCost ForceResult q

-- | noMatCost is the cost of a non-materialized node. Each time it is
-- calculated it imposes a multiplication cap. As this means an
-- exponential effect on the signifiance of a term after a few
-- iterations it is not worth evaluating the actual cost. However we
-- want the actual cap passed over to keep rising to reflect the
-- invariants.
noMatCost
  :: NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) Cost)
  -> NodeProc t n (SumTag (PlanParams n) Cost)

noMatCost _ref matCost = recur
  where
    recur = MealyArrow $ fromKleisli $ \conf -> do
      scale <- ask
      if scale < 0.01 then return (recur,BndRes 0) else local (* factor)
        $ second changeBound
        <$> toKleisli (runMealyArrow matCost) (changeCap conf)
    lowerThresh = 10
    factor = 0.5
    changeBound = \case
      BndBnd (Min' b) -> if b
        > lowerThresh then BndBnd $ Min' $ scaleCost factor b else BndRes 0
      BndRes r -> BndRes $ scaleCost 0.5 <$> r
      e@(BndErr _) -> e
    changeCap conf = conf { confCap = case confCap conf of
      CapVal c    -> CapVal $ scaleCost (1 / factor) <$> c
      ForceResult -> ForceResult
      CapStruct i -> CapStruct i }


scaleCost :: Double -> Cost -> Cost
scaleCost scale Cost {..} =
  Cost { costReads = scaleI costReads,costWrites = scaleI costWrites }
  where
    scaleI x = round $ scale * fromIntegral x
