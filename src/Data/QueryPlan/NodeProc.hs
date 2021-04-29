{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Data.QueryPlan.NodeProc
  (NodeProc) where

import Control.Antisthenis.Minimum
import Control.Antisthenis.Sum
import qualified Control.Category as C
import Data.Utils.Functors
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

type NTrail = NodeSet
-- for example ZEpoch v == RefMap n IsMat
type NodeProc n w = NodeProc0 n w w
type NodeProc0 n w0 w =
  ArrProc
    w
    (FixStateT (NodeProcMap n w0) (State (NTrail n)))

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
data DSet n p v = DSet { dsetConst :: Sum v,dsetNeigh :: [NodeProc n (SumTag p v)] }
makeCostProc
  :: forall v p n .
  (Num v
  ,Ord v
  ,AShow v
  ,Monoid (ZEpoch (SumTag p v))
  ,NoArgError (ExtError p)
  ,Ord (ExtEpoch p)
  ,AShow (ExtError p)
  ,ExtParams p)
  => [DSet n p v]
  -> NodeProc n (SumTag p v)
makeCostProc deps = convArrProc convMinSum $ procMin $ go <$> deps
  where
    go DSet {..} = convArrProc convSumMin $ procSum $ constArr : dsetNeigh
      where
        constArr = arr $ const $ BndRes dsetConst
    procMin :: [NodeProc0 n (SumTag p v) (MinTag p v)]
            -> NodeProc0 n (SumTag p v) (MinTag p v)
    procMin = mkProc
    procSum :: [NodeProc n (SumTag p v)] -> NodeProc n (SumTag p v)
    procSum = mkProc

type family Arr x :: * -> * -> *
type instance Arr (c a b) = c


modTrailE :: (NTrail n -> Either (ZErr w) (NTrail n))
          -> Arr (NodeProc n w) a (Either (ZErr w) a)
modTrailE f = mealyLift $ Kleisli $ \a -> lift (gets f) >>= \case
  Left e -> return $ Left e
  Right r -> lift (put r) >> return (Right a)

modTrail :: (NTrail n -> NTrail n) -> Arr (NodeProc n w) a a
modTrail f = mealyLift $ Kleisli $ \a -> lift (modify f) >> return a
withTrail :: ZErr w -> NodeRef n -> NodeProc n w -> NodeProc n w
withTrail cycleErr ref m =
  modTrailE putRef >>> (arr BndErr ||| m) >>> modTrail (nsDelete ref)
  where
    putRef
      ns = if ref `nsMember` ns then Left cycleErr else Right $ nsInsert ref ns

-- Arrow choice

-- TODO: methods to
--
-- * Register the epoch.
--
-- * Check that the epoch meaningfully changed and return the old
--   result.
--
-- * Connect to the PlanT
