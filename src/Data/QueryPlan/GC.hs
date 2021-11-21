module Data.QueryPlan.GC
  (delSets) where

import           Control.Monad
import           Control.Monad.Extra
import           Data.List
import           Data.List.Extra
import           Data.NodeContainers
import           Data.Query.QuerySize
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.Functors
import           GHC.Generics

data SSet n = SSet { ssMaxSize :: PageNum,ssNodes :: [[NodeRef n]] }
  deriving (Generic,Show,Eq)

instance Semigroup (SSet n) where
  a <> b =
    SSet { ssMaxSize = max (ssMaxSize a) (ssMaxSize b)
          ,ssNodes = ssNodes a ++ ssNodes b
         }

instance Monoid (SSet n) where
  mempty = SSet { ssMaxSize = 0,ssNodes = [] }

singletonSSet :: PageNum -> NodeRef n -> SSet n
singletonSSet s n = SSet { ssMaxSize = s,ssNodes = [[n]] }

nubSSet :: SSet n -> SSet n
nubSSet ss = ss { ssNodes = nubOrd $ ssNodes ss }

mkSSet :: Monad m => NodeRef n -> PlanT t n m (SSet n)
mkSSet ref = do
  refSize <- totalNodePages ref
  ins <- fmap2 (toNodeList . metaOpIn) $ findMetaOps ref
  sets <- forM ins $ \sset -> do
    pgs <- sum <$> traverse totalNodePages sset
    return (pgs,sset)
  return
    $ nubSSet
    $ singletonSSet refSize ref
    <> (case sortOn (negate . fst) sets of
          []           -> mempty
          mx@((s,_):_) -> SSet { ssMaxSize = s,ssNodes = fmap snd mx })

nubOnSSet :: [SSet n] -> [SSet n]
nubOnSSet (x:y:xs) = if x == y then nubOnSSet (x:xs) else x:nubOnSSet (y:xs)
nubOnSSet x        = x

-- | From a list of deletable nodes get a list of node sets. Each set
-- must be deleted entirely or not at all.
delSets :: Monad m => [NodeRef n] -> PlanT t n m [[NodeRef n]]
delSets ns = do
  ssets <- mapM (validSSet ns <=< mkSSet) ns
  return $ ssNodes =<< sortOn ssMaxSize ssets

validSSet :: Monad m => [NodeRef n] -> SSet n -> PlanT t n m (SSet n)
validSSet ns ss = do
  ssn <- filterM (allM validNode) $ ssNodes ss
  return $ ss { ssNodes = ssn }
  where
    validNode ref = do
      st <- getNodeState ref
      -- If there are nomat nodes in the group it means the metaop is
      -- not trigerable anyway. We already have the node on its own
      -- anyway.
      case st of
        Initial Mat -> return $ ref `elem` ns
        _           -> return False
