{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Data.QueryPlan.NodeProc (NodeProc,getCost) where

import Data.Utils.Debug
import Control.Monad.Reader
import Control.Monad.Except
import Data.Utils.Default
import Data.Utils.Functors
import Data.QueryPlan.MetaOp
import Data.QueryPlan.Nodes
import Data.List.NonEmpty as NEL
import Data.Maybe
import Data.QueryPlan.Types
import Data.NodeContainers
import Control.Antisthenis.ATL.Class.Functorial
import Control.Antisthenis.ATL.Common
import Control.Monad.Identity
import Control.Antisthenis.Minimum
import Control.Antisthenis.Convert
import Data.Utils.Monoid
import Data.Utils.AShow
import Control.Antisthenis.Zipper
import Control.Monad.State
import Control.Monad.Writer hiding (Sum)
import Control.Antisthenis.ATL.Transformers.Mealy
import Control.Antisthenis.Types
import Control.Arrow hiding ((>>>))


makeCostProc
  :: forall v t n .
  (Num v,Ord v,AShow v)
  => NodeRef n
  -> [DSet t n (PlanParams n) v]
  -> NodeProc t n (SumTag (PlanParams n) v)
makeCostProc ref deps = convArrProc convMinSum $ procMin $ go <$> deps
  where
    go DSetR {..} = convArrProc convSumMin $ procSum $ constArr : dsetNeigh
      where
        constArr = arr $ const $ BndRes dsetConst
    procMin
      :: [NodeProc0 t n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)]
      -> NodeProc0 t n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)
    procMin = mkProcId ("min:" ++ ashow ref)
    procSum :: [NodeProc t n (SumTag (PlanParams n) v)]
            -> NodeProc t n (SumTag (PlanParams n) v)
    procSum = mkProcId ("sum:" ++ ashow ref)

modTrailE
  :: Monoid (ZCoEpoch w)
  => (NTrail n -> Either (ZErr w) (NTrail n))
  -> Arr (NodeProc t n w) a (Either (ZErr w) a)
modTrailE f = mealyLift $ fromKleisli $ \a -> gets (f . npsTrail) >>= \case
  Left e -> return $ Left e
  Right r -> do
    modify $ \nps -> nps { npsTrail = r }
    return (Right a)

modTrail :: Monoid (ZCoEpoch w)
         => (NTrail n -> NTrail n)
         -> Arr (NodeProc t n w) a a
modTrail f = mealyLift $ fromKleisli $ \a -> do
  modify $ \nps -> nps { npsTrail = f $ npsTrail nps }
  return a
withTrail
  :: Monoid (ZCoEpoch w)
  => (NTrail n -> ZErr w)
  -> NodeRef n
  -> NodeProc t n w
  -> NodeProc t n w
withTrail cycleErr ref m =
  modTrailE putRef >>> (arr BndErr ||| m) >>> modTrail (nsDelete ref)
  where
    putRef ns =
      if ref `nsMember` ns then Left $ cycleErr ns else Right $ nsInsert ref ns

type CostParams n = SumTag (PlanParams n) Cost
-- | Transfer the value of the epoch to the coepoch
mkEpoch
  :: Monoid (ExtCoEpoch (PlanParams n))
  => NodeRef n
  -> Arr
    (NodeProc t n (SumTag (PlanParams n) v))
    (Conf (CostParams n))
    (Either (ZRes (CostParams n)) (Conf (CostParams n)))
mkEpoch ref = mealyLift $ fromKleisli $ \conf -> do
  let isMater = fromMaybe False $ ref `refLU` confEpoch conf
  tell $ refFromAssocs [(ref,isMater)]
  return
    $ if isMater then trace ("materialized: " ++ ashow ref) (Left 0)
    else trace ("Continuing: " ++ ashow ref)
      $ Right conf { confTrPref = ashow ref }

squashMealy
  :: (ArrowFunctor c,Monad (ArrFunctor c))
  => ArrFunctor c (MealyArrow c a b)
  -> MealyArrow c a b
squashMealy m = MealyArrow $ fromKleisli $ \a -> do
  MealyArrow m' <- m
  toKleisli m' a

-- | Build AND INSERT a new mech in the mech directory.
mkNewMech :: NodeRef n -> NodeProc t n (CostParams n)
mkNewMech ref = squashMealy $ do
  mops <- lift2 $ findCostedMetaOps ref
  -- Should never see the same val twice.
  traceM $ "vals: " ++ ashow (ref,toNodeList . metaOpIn . fst <$> mops)
  let mechs =
        [DSetR { dsetConst = Sum $ Just cost
                ,dsetNeigh = [getOrMakeMech n | n <- toNodeList $ metaOpIn mop]
               } | (mop,cost) <- mops]
  let ret =
        withTrail (ErrCycle ref) ref
        $ arr (trace ("Evaluating: " ++ ashow ref))
        >>> mkEpoch ref
        >>> (arr BndRes) ||| makeCostProc ref mechs
        >>> arr (\r -> trace ("Evaluated: " ++ ashow (ref,r)) r)
  lift2 $ modify $ \gcs
    -> gcs { gcMechMap = refInsert ref ret $ gcMechMap gcs }
  return ret

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

getCost :: Monad m => Cap (Min Cost) -> NodeRef n -> PlanT t n m (Maybe Cost)
getCost cap ref = do
  traceM $ "getCost starting: " ++ ashow ref
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  ((res,_coepoch),_trail) <- planQuickRun
    $ (`runStateT` def)
    $ runWriterT
    $ runMech (getOrMakeMech ref)
    $ Conf { confCap = cap,confEpoch = states,confTrPref = ashow ref }
  traceM $ "getCost returning: " ++ ashow (ref,res)
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing) -> return $ Just 0
    BndBnd _bnd -> return Nothing
    BndErr e -> throwPlan $ "antisthenis error: " ++ ashow e

-- Arrow choice

-- TODO: methods to
--
-- * Fix the trail (done)
--
-- * Check that the epoch meaningfully changed and return the old
--   result. Use the ZipperMonadExt implementation. (done)
--
-- * Connect to the PlanT (done)
--
-- * Connect to isMatable
