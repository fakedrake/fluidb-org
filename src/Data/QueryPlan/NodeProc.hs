{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Data.QueryPlan.NodeProc
  (NodeProc) where

import Control.Antisthenis.Convert
import Data.Utils.Monoid
import Data.Utils.AShow
import Control.Antisthenis.Zipper
import Control.Monad.State
import Control.Monad.Writer hiding (Sum)
import Control.Antisthenis.ATL.Transformers.Mealy
import Data.Utils.FixState
import Control.Antisthenis.Types
import Control.Arrow
import Control.Monad.Reader
import Data.NodeContainers

-- | A map containing all the proc maps. Mutually recursive with the
-- proc itself.
newtype NodeProcMap n w m =
  NodeProcMap { unNodeProcMap :: RefMap n (ArrProc w m) }

data EvalConf n = EvalConf { ecTrail :: NodeSet n }
-- for example ZEpoch v == RefMap n IsMat
type NodeProc n w = NodeProc0 n w w
type NodeProc0 n w0 w =
  ArrProc
    w
    (FixStateT (NodeProcMap n w0) (ReaderT (EvalConf n) (Writer (ZEpoch w0))))

-- | Lookup a node process. If you can't find one substitute for the one
-- provided.
luNodeProc :: Monoid (ZEpoch w) => NodeProc n w -> NodeRef n -> NodeProc n w
luNodeProc (MealyArrow (Kleisli mkProc)) k = MealyArrow $ Kleisli go
  where
    go conf = gets (refLU k . unNodeProcMap) >>= \case
      Nothing -> mkProc conf
      Just (MealyArrow (Kleisli c)) -> do
        (nxt,r) <- c conf
        modify $ NodeProcMap . refInsert k nxt . unNodeProcMap
        return (MealyArrow $ Kleisli go,r)

-- | depset has a constant part that is the cost of triggering and a
-- variable part.
data DSet n v = DSet { dsetConst :: Sum v,dsetNeigh :: [NodeProc n (Sum v)] }
makeCostProc
  :: forall v n .
  (Num v,Ord v,AShow v,Monoid (ZEpoch (Sum v)))
  => [DSet n v]
  -> NodeProc n (Sum v)
makeCostProc deps = convArrProc convMinSum $ procMin $ go <$> deps
  where
    go DSet {..} = convArrProc convSumMin $ procSum $ constArr : dsetNeigh
      where
        constArr = arr $ const $ BndRes dsetConst
    procMin :: [NodeProc0 n (Sum v) (Min v)]
            -> NodeProc0 n (Sum v) (Min v)
    procMin = mkProc
    procSum :: [NodeProc n (Sum v)] -> NodeProc n (Sum v)
    procSum = mkProc


-- TODO: methods to
--
-- * Register the epoch.
--
-- * Check that the epoch meaningfully changed and return the old
--   result.
--
-- *
