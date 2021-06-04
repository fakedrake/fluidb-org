module Data.QueryPlan.Materializable
  (isMaterializable) where

import Control.Monad.Identity
import Control.Antisthenis.Bool
import Control.Antisthenis.Types
import Data.List.NonEmpty as NEL
import Data.QueryPlan.Types
import Data.NodeContainers

isMaterializable :: Monad m => NodeRef n -> PlanT t n m Bool
isMaterializable cap ref = do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  ((res,_coepoch),_trail) <- planQuickRun
    $ (`runStateT` def)
    $ runWriterT
    $ runMech (getOrMakeMech ref)
    $ Conf
    { confCap = cap
     ,confEpoch = states
     ,confTrPref = "topLevel:" ++ ashow ref
    }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing) -> return $ Just 0
    BndBnd _bnd -> return Nothing
    BndErr e -> throwPlan $ "antisthenis error: " ++ ashow e

getOrMakeMech
  :: NodeRef n -> NodeProc t n (SumTag (PlanParams n) Cost)
getOrMakeMech ref =
  squashMealy
  $ lift2
  $ gets
  $ fromMaybe (mkNewMech ref) . refLU ref . gcMechMap


-- | Run PlanT in the Identity monad.
planQuickRun :: Monad m => PlanT t n Identity a -> PlanT t n m a
planQuickRun m = do
  st0 <- get
  conf <- ask
  case runIdentity $ runExceptT $ (`runReaderT` conf) $ (`runStateT` st0) m of
    Left e -> throwError e
    Right (a,st) -> put st >> return a
