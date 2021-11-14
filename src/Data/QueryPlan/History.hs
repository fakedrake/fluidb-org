{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies     #-}
module Data.QueryPlan.History (pastCosts) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Types
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.IntSet                                as IS
import           Data.NodeContainers
import           Data.Pointed
import           Data.Proxy
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.HistBnd
import           Data.QueryPlan.NodeProc
import           Data.QueryPlan.PlanMech
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.ListT
import           Data.Utils.Nat


-- type HCost = Cert (Comp Cost)
instance PlanMech (PlanT t n Identity) (CostParams HistTag n) n where
  mcGetMech Proxy ref = gets $ refLU ref . gcHistMechMap
  mcPutMech Proxy ref me =
    modify $ \gc -> gc { gcHistMechMap = refInsert ref me $ gcHistMechMap gc }
  mcMkCost Proxy _ref cost = point cost
  mcIsMatProc Proxy = isMatCost
  mcCompStackVal Proxy _ref = BndRes $ point nonComp
  mcIsComputable _ = \case
    BndErr _                                  -> False
    BndRes (Sum (Just HistVal {hvNonComp=c})) -> c < 0.6
    _                                         -> True

-- | The expected cost of the next query.
pastCosts
  :: forall t n m .
  Monad m
  => Cost
  -> ListT (PlanT t n m) (Maybe (HistVal Cost))
pastCosts maxCost = do
  QueryHistory qs <- asks queryHistory
  lift $ trM $ "History size: " ++ ashow (length qs)
  q <- mkListT $ return $ take 3 qs
  res <- lift
    $ wrapTraceT ("antisthenis:pastCost" <: q)
    $ getPlanBndR @(CostParams HistTag n) Proxy (CapVal $ maxCap maxCost) q
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing) -> return Nothing
    BndBnd _bnd -> return Nothing
    BndErr
      e -> error $ "getCost(" ++ ashow q ++ "):antisthenis error: " ++ ashow e

-- | The materialized node indeed has a cost. That cost is
-- calculated by imposing a scaling on the cost it would have if it
-- were not materialized. This opens the door to an explosion in
-- computation.
isMatCost :: forall t n . NodeRef n -> HistProc t n -> HistProc t n
isMatCost _ref matCost0 = wrapMealy matCost0 $ \conf matCost -> if isTooDeep
  conf then zeroRes matCost else do
  -- "Mat historical" <<: ref
  let conf0 = mapCap unscaleCap conf
  -- The cap used to run the arror will not match the scaled
  -- result. If that happens the outside process will fail to
  -- proceed and will keep asking for a larger result.
  (nxt,r) <- lift $ toKleisli (runMealyArrow matCost) conf0
  -- "result" <<: (ref,confCap conf,confCap conf0,r)
  conf' <- yieldMB $ case r of
    BndBnd (Min (Just bnd)) -> BndBnd $ point $ incrementMat $ scaleHistVal bnd
    BndBnd (Min Nothing)    -> BndBnd $ Min Nothing
    BndRes res              -> BndRes $ scaleSum res
    -- error is more highly non-computable but it's fre
    _e                      -> BndRes $ point $ zero { hvNonComp = 0.5 }
  return (conf',nxt)
  where
    zeroRes matCost = do
      conf' <- yieldMB $ BndRes zero
      return (conf',matCost)
    isTooDeep c = case confCap c of
      CapVal cap  -> hcMatsEncountered cap >= 3
      ForceResult -> False

-- Nothe that the error is thoun automatically if it is not
-- computable.

incrementMat :: HistVal a -> HistVal a
incrementMat hv =
  hv { hvMaxMatTrail =
         IS.singleton $ maybe 1 (1 +) $ fst <$> IS.maxView (hvMaxMatTrail hv)
     }
-- | we may be overshooting here and there but it's ok
mapCap :: (ZCap w -> ZCap w) -> Conf w -> Conf w
mapCap f conf = conf { confCap = f <$> confCap conf }
