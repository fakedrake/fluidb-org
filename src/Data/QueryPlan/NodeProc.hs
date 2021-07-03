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
module Data.QueryPlan.NodeProc (NodeProc,getCost) where

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
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.ProcTrail
import           Data.QueryPlan.Types
import           Data.Utils.AShow
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
        mid = "sum:" ++ ashow ref

type CostParams n = SumTag (PlanParams n) Cost
type NoMatProc t n =
  NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) Cost) -- The mat proc constructed by the node under consideration
  -> NodeProc t n (SumTag (PlanParams n) Cost)

-- | Build AND INSERT a new mech in the mech directory.
mkNewMech
  :: NoMatProc t n
  -> NodeRef n
  -> NodeProc t n (CostParams n)
mkNewMech noMatProc ref = squashMealy $ do
  mops <- lift3 $ findCostedMetaOps ref
  -- Should never see the same val twice.
  let mechs =
        [DSetR { dsetConst = Sum $ Just cost
                ,dsetNeigh =
                   [getOrMakeMech noMatProc n | n <- toNodeList $ metaOpIn mop]
               } | (mop,cost) <- mops]
  let costProcess = makeCostProc ref mechs
  let ret =
        withTrail (ErrCycle ref) ref
        $ mkEpoch id ref >>> noMatProc ref costProcess ||| costProcess
  lift2 $ modify $ \gcs
    -> gcs { gcMechMap = refInsert ref ret $ gcMechMap gcs }
  return ret

getOrMakeMech
  :: NoMatProc t n -> NodeRef n -> NodeProc t n (SumTag (PlanParams n) Cost)
getOrMakeMech noMatProc ref =
  squashMealy
  $ lift2
  $ gets
  $ fromMaybe (mkNewMech noMatProc ref) . refLU ref . gcMechMap


-- | Run PlanT in the Identity monad.
planQuickRun :: Monad m => PlanT t n Identity a -> PlanT t n m a
planQuickRun m = do
  st0 <- get
  conf <- ask
  case runIdentity $ runExceptT $ (`runReaderT` conf) $ (`runStateT` st0) m of
    Left e       -> throwError e
    Right (a,st) -> put st >> return a

getCost
  :: Monad m
  => NoMatProc t n
  -> Cap (Min' Cost)
  -> NodeRef n
  -> PlanT t n m (Maybe Cost)
getCost noMatProc cap ref = do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  ((res,_coepoch),_trail) <- planQuickRun
    $ (`runReaderT` 1)
    $ (`runStateT` def)
    $ runWriterT
    $ runMech (getOrMakeMech noMatProc ref)
    $ Conf
    { confCap = cap,confEpoch = states,confTrPref = "topLevel:" ++ ashow ref }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing)  -> return $ Just 0
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
