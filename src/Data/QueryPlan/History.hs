module Data.QueryPlan.History
  (pastCosts) where

import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import           Control.Monad.Reader
import           Data.NodeContainers
import           Data.Profunctor
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.ListT

-- | The expected cost of the next query.
pastCosts :: Monad m => ListT (PlanT t n m) (Maybe Cost)
pastCosts = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return qs
  lift $ getCost noMatCost ForceResult q

-- XXX: when cap is below the threshold just return 0.
noMatCost
  :: NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) Cost)
  -> NodeProc t n (SumTag (PlanParams n) Cost)
noMatCost _ref = dimap changeCap changeBound
  where
    changeBound = \case
      BndBnd b     -> BndBnd $ scaleCost 0.5 <$> b
      BndRes r     -> BndRes $ scaleCost 0.5 <$> r
      e@(BndErr _) -> e
    changeCap conf = conf { confCap = case confCap conf of
      CapVal c    -> CapVal $ scaleCost 2 <$> c
      ForceResult -> ForceResult
      CapStruct _ -> CapStruct 0 }
scaleCost :: Double -> Cost -> Cost
scaleCost scale Cost {..} =
  Cost { costReads = scaleI costReads,costWrites = scaleI costWrites }
  where
    scaleI x = round $ scale * fromIntegral x
