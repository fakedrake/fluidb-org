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
import           Data.List.NonEmpty                         as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Profunctor
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.ProcTrail
import           Data.QueryPlan.Types
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
makeCostProc ref deps = convArrProc convMinSum $ procMin $ go <$> deps
  where
    go DSetR {..} = convArrProc convSumMin $ procSum $ constArr : dsetNeigh
      where
        constArr = arr $ const $ BndRes dsetConst
    procMin
      :: [NodeProc0 t n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)]
      -> NodeProc0 t n (SumTag (PlanParams n) v) (MinTag (PlanParams n) v)
    procMin ns = lmap (\conf -> conf { confTrPref = mid }) $ mkProcId mid ns
      where
        mid = "min:" ++ ashow ref
    procSum :: [NodeProc t n (SumTag (PlanParams n) v)]
            -> NodeProc t n (SumTag (PlanParams n) v)
    procSum ns = lmap (\conf -> conf { confTrPref = mid }) $ mkProcId mid ns
      where
        mid = "sum:" ++ ashow ref

type CostParams n v = SumTag (PlanParams n) v
type NoMatProc t n v =
  NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) v) -- The mat proc constructed by the node under consideration
  -> NodeProc t n (SumTag (PlanParams n) v)

-- | Build AND INSERT a new mech in the mech directory.
mkNewMech
  :: (Invertible v,Ord2 v v,AShow v)
  => MechConf t n v
  -> NodeRef n
  -> NodeProc t n (CostParams n v)
mkNewMech mc@MechConf {..} ref = squashMealy $ do
  mops <- lift3 $ findCostedMetaOps ref
  -- Should never see the same val twice.
  let mechs =
        [DSetR
          { dsetConst = Sum $ Just $ mcMkCost ref cost
           ,dsetNeigh = [getOrMakeMech mc n | n <- toNodeList $ metaOpIn mop]
          } | (mop,cost) <- mops]
  let costProcess = makeCostProc ref mechs
  let ret =
        withTrail (ErrCycle ref) ref
        $ mkEpoch id ref >>> mcIsMatProc ref costProcess ||| costProcess
  lift2 $ modify $ modL mcMechMapLens $ refInsert ref ret
  return ret

data MechConf t n v =
  MechConf
  { mcMechMapLens :: GCState t n :>: RefMap n (NodeProc t n (CostParams n v))
   ,mcMkCost      :: NodeRef n -> Cost -> v
   ,mcIsMatProc   :: NoMatProc t n v
  }

data a :>:  b = Lens { getL :: a -> b,modL :: (b -> b) -> a -> a }

getOrMakeMech
  :: (Invertible v,Ord2 v v,AShow v)
  => MechConf t n v
  -> NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) v)
getOrMakeMech mc@MechConf {..} ref =
  squashMealy
  $ lift2
  $ gets
  $ fromMaybe (mkNewMech mc ref) . refLU ref . getL mcMechMapLens


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
getCost mc extraMat cap ref = wrapTrace ("getcost: " ++ show ref) $ do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  let extraStates = nsToRef (const True) extraMat
  ((res,_coepoch),_trail) <- planQuickRun
    $ (`runReaderT` 1)
    $ (`runStateT` def)
    $ runWriterT
    $ runMech (getOrMakeMech mc ref)
    $ Conf
    { confCap = cap
     ,confEpoch = states <> extraStates
     ,confTrPref = "topLevel:" ++ ashow ref
    }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing)  -> return $ Just mempty
    BndBnd _bnd           -> return Nothing
    BndErr e              -> throwPlan $ "antisthenis error: " ++ ashow e

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
-- mkCost cost ref = PlanCost { pcCost = cost,pcPlan = nsSingleton ref }
