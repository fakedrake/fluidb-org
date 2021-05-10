module Data.QueryPlan.History (futureCost,expectedCost) where

import Data.NodeContainers
import Control.Monad.Reader
import Data.QueryPlan.Types

-- | The expected cost of the next query.
futureCost :: Monad m => PlanT t n m Cost
futureCost = do
  QueryHistory qs <- asks queryHistory
  fmap mconcat $ forM qs $ \q -> do
    expectedCost q


-- | Incrementally compute the expected cost of a node. Lookup the
-- arrow. If you can't find it, create and insert it. Then run it.
expectedCost :: NodeRef n -> PlanT t n m Cost
expectedCost = undefined
