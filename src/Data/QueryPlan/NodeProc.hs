{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Data.QueryPlan.NodeProc
  (NodeProc) where

import Control.Antisthenis.Convert
import Data.Profunctor
import Data.Utils.Monoid
import Data.Utils.AShow
import Control.Antisthenis.Zipper
import Control.Antisthenis.Minimum
import Control.Antisthenis.Sum
import Control.Monad.State
import Control.Monad.Writer hiding (Sum)
import Control.Antisthenis.ATL.Transformers.Mealy
import Data.Utils.FixState
import Control.Antisthenis.Types
import Control.Arrow
import Control.Monad.Reader
import Data.NodeContainers

newtype NodeProcMap n w m = NodeProcMap { unNodeProcMap :: RefMap n (ArrProc w m) }

-- for example ZEpoch v == RefMap n Version
type NodeProc n w = NodeProc0 n w w
type NodeProc0 n w0 w =
  ArrProc
    w
    (FixStateT (NodeProcMap n w0) (ReaderT (NodeSet n) (Writer (ZEpoch w0))))

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


makeCostProc
  :: forall v n .
  (Num v,Ord v,AShow v,Monoid (ZEpoch (Sum v)))
  => [[NodeProc n (Sum v)]]
  -> NodeProc n (Sum v)
makeCostProc neighbors =
  convArrProc convMinSum
  $ procMin
  $ convArrProc convSumMin . procSum <$> neighbors
  where
    procMin :: [NodeProc0 n (Sum v) (Min v)]
            -> NodeProc0 n (Sum v) (Min v)
    procMin = mkProc
    procSum :: [NodeProc n (Sum v)] -> NodeProc n (Sum v)
    procSum = mkProc
