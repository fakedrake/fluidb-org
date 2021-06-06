{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
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

import Data.QueryPlan.ProcCommon
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



-- | depset has a constant part that is the cost of triggering and a
-- variable part.
data DSetR v p = DSetR { dsetConst :: Sum v,dsetNeigh :: [p] }
  deriving (Functor,Foldable,Traversable)

-- | The dependency set in terms of processes.
type DSet t n p v = DSetR v (NodeProc t n (SumTag p v))

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
    procMin ns = arr (\conf -> conf { confTrPref = mid }) >>> mkProcId mid ns
      where
        mid = "min:" ++ ashow ref
    procSum :: [NodeProc t n (SumTag (PlanParams n) v)]
            -> NodeProc t n (SumTag (PlanParams n) v)
    procSum ns = arr (\conf -> conf { confTrPref = mid }) >>> mkProcId mid ns
      where
        mid = ("sum:" ++ ashow ref)

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
  return $ if isMater then Left 0 else Right conf


-- | Build AND INSERT a new mech in the mech directory.
mkNewMech :: NodeRef n -> NodeProc t n (CostParams n)
mkNewMech ref = squashMealy $ do
  mops <- lift2 $ findCostedMetaOps ref
  -- Should never see the same val twice.
  let mechs =
        [DSetR { dsetConst = Sum $ Just cost
                ,dsetNeigh = [getOrMakeMech n | n <- toNodeList $ metaOpIn mop]
               } | (mop,cost) <- mops]
  let ret =
        withTrail (ErrCycle ref) ref
        $ mkEpoch ref >>> (arr BndRes) ||| makeCostProc ref mechs
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

getCost :: Monad m => Cap (Min' Cost) -> NodeRef n -> PlanT t n m (Maybe Cost)
getCost cap ref = do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  ((res,_coepoch),_trail) <- planQuickRun
    $ (`runStateT` def)
    $ runWriterT
    $ runMech (getOrMakeMech ref)
    $ Conf
    { confCap = cap,confEpoch = states,confTrPref = "topLevel:" ++ ashow ref }
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
