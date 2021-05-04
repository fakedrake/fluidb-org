{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Data.QueryPlan.NodeProc
  (NodeProc) where

import Control.Antisthenis.ATL.Class.Functorial
import Control.Antisthenis.ATL.Common
import Control.Monad.Identity
import Data.Proxy
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
import Control.Arrow hiding ((>>>))
import Control.Monad.Reader
import Data.NodeContainers

-- | A map containing all the proc maps. Mutually recursive with the
-- proc itself.
data NodeProcSt n w m =
  NodeProcSt
  { npsProcs :: RefMap n (ArrProc w m)
   ,npsTrail :: NTrail n
   ,npsEpoch :: ZEpoch w
  }

type NTrail = NodeSet
-- for example ZEpoch v == RefMap n IsMat
type NodeProc n w = NodeProc0 n w w
type NodeProc0 n w0 w =
  ArrProc
    w
    (FixStateT (NodeProcSt n w0) Identity)

-- | Lookup a node process. If you can't find one substitute for the one
-- provided.
luNodeProc :: Monoid (ZCoEpoch w) => NodeProc n w -> NodeRef n -> NodeProc n w
luNodeProc (MealyArrow (toKleisli -> mkProc)) k = go
  where
    go = MealyArrow $ fromKleisli $ \conf -> gets (refLU k . npsProcs) >>= \case
      Nothing -> mkProc conf
      Just (MealyArrow (toKleisli -> c)) -> do
        (nxt,r) <- c conf
        modify $ \nps -> nps { npsProcs = refInsert k nxt $ npsProcs nps }
        return (go,r)

-- | depset has a constant part that is the cost of triggering and a
-- variable part.
data DSet n p v =
  DSet { dsetConst :: Sum v,dsetNeigh :: [NodeProc n (SumTag p v)] }
makeCostProc
  :: forall v n .
  (Num v,Ord v,AShow v)
  => [DSet n (PlanParams n) v]
  -> NodeProc n (SumTag (PlanParams n) v)
makeCostProc deps = convArrProc convMinSum $ procMin $ go <$> deps
  where
    go DSet {..} = convArrProc convSumMin $ procSum $ constArr : dsetNeigh
      where
        constArr = arr $ const $ BndRes dsetConst
    procMin :: [NodeProc0 n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)]
            -> NodeProc0 n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)
    procMin = mkProc
    procSum :: [NodeProc n (SumTag (PlanParams n) v)]
            -> NodeProc n (SumTag (PlanParams n) v)
    procSum = mkProc


modTrailE
  :: Monoid (ZCoEpoch w)
  => (NTrail n -> Either (ZErr w) (NTrail n))
  -> Arr (NodeProc n w) a (Either (ZErr w) a)
modTrailE f = mealyLift $ fromKleisli $ \a -> gets (f . npsTrail) >>= \case
  Left e -> return $ Left e
  Right r -> do
    modify $ \nps -> nps { npsTrail = r }
    return (Right a)

modTrail :: Monoid (ZCoEpoch w)
         => (NTrail n -> NTrail n)
         -> Arr (NodeProc n w) a a
modTrail f = mealyLift $ fromKleisli $ \a -> do
  modify $ \nps -> nps { npsTrail = f $ npsTrail nps }
  return a
withTrail
  :: Monoid (ZCoEpoch w) => ZErr w -> NodeRef n -> NodeProc n w -> NodeProc n w
withTrail cycleErr ref m =
  modTrailE putRef >>> (arr BndErr ||| m) >>> modTrail (nsDelete ref)
  where
    putRef
      ns = if ref `nsMember` ns then Left cycleErr else Right $ nsInsert ref ns

mkEpoch
  :: Monoid (ExtCoEpoch (PlanParams n))
  => NodeRef n
  -> Bool
  -> Arr (NodeProc n (SumTag (PlanParams n) v)) a a
mkEpoch ref val = mealyLift $ fromKleisli $ \x -> do
  modify $ \nps -> nps { npsEpoch = refInsert ref val $ npsEpoch nps }
  return x

data PlanParams n

instance ExtParams (PlanParams n) where
  type ExtError (PlanParams n) = Err
  type ExtEpoch (PlanParams n) = RefMap n Bool
  type ExtCoEpoch (PlanParams n) = RefMap n Bool


-- Arrow choice

-- TODO: methods to
--
-- * Fix the trail (done)
--
-- * Check that the epoch meaningfully changed and return the old
--   result. Use the ZipperMonadExt implementation.
--
-- * Connect to the PlanT
