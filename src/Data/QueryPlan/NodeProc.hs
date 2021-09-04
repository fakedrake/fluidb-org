{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Data.QueryPlan.NodeProc (NodeProc,getCost,MechConf(..),(:>:)(..)) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Common
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Convert
import           Control.Antisthenis.Minimum
import           Control.Antisthenis.Types
import           Control.Antisthenis.Zipper
import           Control.Arrow                              hiding ((>>>))
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer                       hiding (Sum)
import qualified Data.List.NonEmpty                         as NEL
import           Data.NodeContainers
import           Data.Profunctor
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.ProcTrail
import           Data.QueryPlan.Types
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Monoid


-- | depset has a constant part that is the cost of triggering and a
-- variable part.
data DSetR v p = DSetR { dsetConst :: Sum v,dsetNeigh :: [p] }
  deriving (Functor,Foldable,Traversable)

-- | The dependency set in terms of processes.
type DSet t n p v = DSetR v (NodeProc t n (SumTag p v))

makeCostProc
  :: forall v t n .
  (Invertible v,Ord2 v v,AShow v)
  => NodeRef n
  -> [DSet t n (PlanParams n) v]
  -> NodeProc t n (SumTag (PlanParams n) v)
makeCostProc ref deps =
  convArrProc convMinSum $ procMin $ zipWith go [1 ..] deps
  where
    go i DSetR {..} = convArrProc convSumMin $ procSum i $ constArr : dsetNeigh
      where
        constArr = arr $ const $ BndRes dsetConst
    procMin
      :: [NodeProc0 t n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)]
      -> NodeProc0 t n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)
    procMin ns =
      lmap (\conf -> conf { confTrPref = zidName mid : confTrPref conf })
      $ mkProcId mid ns
      where
        mid = zidDefault $ "min:" ++ ashow ref
    procSum :: Int
            -> [NodeProc t n (SumTag (PlanParams n) v)]
            -> NodeProc t n (SumTag (PlanParams n) v)
    procSum i ns =
      lmap (\conf -> conf { confTrPref = zidName mid : confTrPref conf })
      $ mkProcId mid ns
      where
        mid = zidDefault $ "sum[" ++ show i ++ "]:" ++ ashow ref

type CostParams n v = SumTag (PlanParams n) v
type NoMatProc t n v =
  NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) v) -- The mat proc constructed by the node under consideration
  -> NodeProc t n (SumTag (PlanParams n) v)

-- | Build AND INSERT a new mech in the mech directory. The mech does
-- not update it's place in the mech map.
mkNewMech
  :: forall t n v .
  (Invertible v,Ord2 v v,AShow v)
  => MechConf t n v
  -> NodeRef n
  -> NodeProc t n (CostParams n v)
mkNewMech mc@MechConf {..} ref = squashMealy $ do
  mops <- lift3 $ findCostedMetaOps ref
  traceM $ ashow mops
  -- Should never see the same val twice.
  let mechs =
        [DSetR { dsetConst = Sum $ Just $ mcMkCost ref cost
                ,dsetNeigh =
                   [getOrMakeMech ref mc n | n <- toNodeList $ metaOpIn mop]
               } | (mop,cost) <- mops]
  let costProcess = makeCostProc ref mechs
  -- XXX: If we have ephemeral errors we do not need a trail.
  let mkErr trail = ErrCycle ref trail
  let ret =
        asSelfUpdating
        -- $ withTrail mkErr ref
        $ mkEpoch id ref >>> mcIsMatProc ref costProcess ||| costProcess
  return ret
  where
    asSelfUpdating :: NodeProc t n (CostParams n v)
                   -> NodeProc t n (CostParams n v)
    asSelfUpdating (MealyArrow f) = MealyArrow $ fromKleisli $ \c -> do
      (nxt,r) <- toKleisli f c
      let nxt' = asSelfUpdating nxt
      traceM $ "updateMechMap " ++ ashow (ref,r)
      lift2 $ modify $ modL mcMechMapLens $ refInsert ref nxt'
      return (nxt',r)

data MechConf t n v =
  MechConf
  { mcMechMapLens :: GCState t n :>: RefMap n (NodeProc t n (CostParams n v))
   ,mcMkCost      :: NodeRef n -> Cost -> v
   ,mcIsMatProc   :: NoMatProc t n v
  }

data a :>:  b = Lens { getL :: a -> b,modL :: (b -> b) -> a -> a }

-- | Bug(?): in the first round we lookup the mech but in subsequent
-- rounds we are not.
getOrMakeMech
  :: (Invertible v,Ord2 v v,AShow v)
  => NodeRef n
  -> MechConf t n v
  -> NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) v)
getOrMakeMech parentRef mc@MechConf {..} ref = squashMealy $ do
  lift2 (gets $ refLU ref . getL mcMechMapLens) >>= \case
    Nothing -> do
      traceM $ "getOrMakeMech:new-mech: " ++ ashow (parentRef,ref)
      lift2 $ modify $ modL mcMechMapLens $ refInsert ref $ cycleProc ref
      return $ mkNewMech mc ref
    Just x -> do
      traceM $ "getOrMakeMech:found-mech: " ++ ashow (parentRef,ref)
      -- Note: One might think that inserting an temporary error here
      -- might affect correctness. This argument goes something like
      -- this: the value returned by this mech (the temp error) will
      -- be used by a nested computation. However, when this actually
      -- returns a non-temporary value it may very well not be an
      -- error. This means that during the computatuion this cell
      -- CHANGED ITS VALUE which is supposedly not possible since each
      -- cell only has ONE correct concrete value. This argument is
      -- logical but it does not hold because the temporary cycle
      -- error has a lifespan exacly equal to the time it takes to
      -- come up with ANY value for the node's computation. When the
      -- computation comes up with a value, concrete or just a bound,
      -- it will be overwritten and that value WILL actually be
      -- correct. We know that all triggered mechs will have returned
      -- some value before getCost finishes so at the end all mechs
      -- will contain a correct value.
      --
      -- All this also means that it is not possible for a node
      -- process to be rotated independently from two different places
      -- because it can't be looked up.
      lift2 $ modify $ modL mcMechMapLens $ refInsert ref $ cycleProc ref
      return x

cycleProc :: NodeRef n -> NodeProc t n (SumTag (PlanParams n) v)
cycleProc ref =
  arr
  $ const
  $ BndErr
  $ ErrCycle ref mempty

planQuickRun :: Monad m => PlanT t n Identity a -> PlanT t n m a
planQuickRun m = do
  st0 <- get
  conf <- ask
  case runIdentity $ runExceptT $ (`runReaderT` conf) $ (`runStateT` st0) m of
    Left e       -> throwError e
    Right (a,st) -> put st >> return a

getCost
  :: (Invertible v,Ord2 v v,AShow v,Monad m,Monoid v)
  => MechConf t n v
  -> NodeSet n
  -> Cap (Min' v)
  -> NodeRef n
  -> PlanT t n m (Maybe v)
getCost mc extraMat cap ref = wrapTrace ("Top: getCost " ++ show ref) $ do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  let extraStates = nsToRef (const True) extraMat
  traceM $ "Mat: " ++ ashow (states,extraStates)
  ((res,_coepoch),_trail) <- planQuickRun
    $ (`runReaderT` 1)
    $ (`runStateT` def)
    $ runWriterT
    $ runMech (getOrMakeMech ref mc ref)
    $ Conf
    { confCap = cap
     ,confEpoch = states <> extraStates
     ,confTrPref = ["topLevel:" ++ ashow ref]
    }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing)  -> return $ Just mempty
    BndBnd _bnd           -> return Nothing
    BndErr e              -> throwPlan $ "antisthenis error: " ++ ashow e
