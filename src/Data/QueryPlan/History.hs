{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies     #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Data.QueryPlan.History
  (pastCosts) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Types
import           Control.Applicative
import           Control.Monad.Reader
import           Data.NodeContainers
import           Data.Pointed
import           Data.Proxy
import           Data.QueryPlan.Comp
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.Scalable
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.ListT
import           Data.Utils.Nat
import           GHC.Generics

-- | The trailsize indicates "how many" (maximum depth) materialized
-- nodes were traversed to come up with this value. It is only useful
-- in conjuntion with the cap.
data Cert a = Cert { cTrailSize :: Int,cData :: a }
  deriving (Eq,Generic,Functor,Show)
instance AShow a => AShow (Cert a)
instance Pointed Cert where point = Cert 0
instance Zero a => Zero (Cert a) where zero = point zero
-- There are no circumnstances under which we are going to chose a
-- value that has come throgh more materialied nodes. The scaling will
-- have damped it enough.
instance Ord a => Ord (Cert a) where
  compare (Cert _ a') (Cert _ b') = compare a' b'
instance Semigroup a => Semigroup (Cert a) where (<>) = liftA2 (<>)
instance Applicative Cert where
  pure = point
  Cert a f <*> Cert b x = Cert (max a b) $ f x
instance Scalable a => Scalable (Cert a) where scale sc = fmap $ scale sc
instance Subtr a => Subtr (Cert a) where subtr = liftA2 subtr

type HCost = Comp (Cert Cost)
instance PlanMech HistTag n where
  type PlanMechVal HistTag n = HCost
  mcMechMapLens = Lens { getL = gcHistMechMap,modL = \f gc ->
    gc { gcHistMechMap = f $ gcHistMechMap gc } }
  mcMkCost Proxy _ref cost = toComp $ point cost
  mcIsMatProc = isMatCost
  mcCompStackVal _ref = BndRes $ point nonComp

instance HasLens (Min (Comp Cost)) (Min (Comp Cost))
-- | The expected cost of the next query.
pastCosts :: Monad m => NodeSet n -> ListT (PlanT t n m) (Maybe HCost)
pastCosts extraMat = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return qs
  lift
    $ wrapTrace ("pastCosts " ++ ashow q)
    $ getCost @HistTag Proxy extraMat ForceResult q

incrMats :: Conf (CostParams HistTag n)
         -> Conf (CostParams HistTag n)
incrMats conf =
  conf { confCap = case confCap conf of
    ForceResult -> CapVal HistCap { hcMatsEncountered = 1,hcValCap = MinInf }
    CapVal
      cap -> CapVal $ cap { hcMatsEncountered = 1 + hcMatsEncountered cap } }

-- | The materialized node indeed has a cost. That cost is
-- calculated by imposing a scaling on the cost it would have if it
-- were not materialized. This opens the door to an explosion in
-- computation.
isMatCost :: forall t n . NodeRef n -> HistProc t n -> HistProc t n
isMatCost _ref matCost0 = wrapMealy matCost0 guardedGo
  where
    go conf matCost = do
      (nxt,r) <- lift $ toKleisli (runMealyArrow matCost) $ incrMats conf
      conf' <- yieldMB $ case r of
        BndBnd bnd -> BndBnd $ incrementCert $ scaleMin bnd
        BndRes res -> BndRes $ scaleSum res
        _e         -> BndRes $ point $ Comp 0.5 zero -- error is more highly non-computable but it's free
      return (conf',nxt)
    guardedGo conf matCost = if isTooDeep conf then do
      conf' <- yieldMB $ BndRes zero
      return (conf',matCost) else go conf matCost
    isTooDeep c = case confCap c of
      CapVal cap  -> hcMatsEncountered cap > 3
      ForceResult -> False

-- Nothe that the error is thoun automatically if it is not
-- computable.

incrementCert :: Min HCost -> Min HCost
incrementCert (Min (Just (Comp d (Cert c i)))) =
  Min (Just (Comp d (Cert (c + 1) i)))
incrementCert a = a
matComputability :: Double -> Double
matComputability d = 1 - 0.8 * (1 - d)
scaleCost :: Cost -> Cost
scaleCost (Cost r w) = Cost (r `div` 2) (w `div` 2)

scaleMin :: Min HCost -> Min HCost
scaleMin m@(Min Nothing)         = m
scaleMin (Min (Just  (Comp d (Cert c i)))) =
  Min $ Just $ Comp (matComputability d) (Cert c $ scaleCost i)
scaleSum :: Sum HCost -> Sum HCost
scaleSum m@(Sum Nothing)         = m
scaleSum (Sum (Just (Comp d (Cert c i)))) =
  Sum $ Just $ Comp (matComputability d) (Cert c $ scaleCost i)
