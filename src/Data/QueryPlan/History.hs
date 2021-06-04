module Data.QueryPlan.History
  (pastCosts) where

import Data.Utils.AShow
import Data.Utils.ListT
import Control.Antisthenis.Types
import Data.QueryPlan.NodeProc
import Control.Monad.Reader
import Data.QueryPlan.Types

-- | The expected cost of the next query.
pastCosts :: Monad m => ListT (PlanT t n m) (Maybe Cost)
pastCosts = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return qs
  lift $ getCost ForceResult q
