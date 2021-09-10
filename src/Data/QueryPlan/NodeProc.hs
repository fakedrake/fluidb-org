{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE ImplicitParams        #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Data.QueryPlan.NodeProc (NodeProc,getCost,MechConf(..),(:>:)(..)) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Writer
import           Control.Antisthenis.Convert
import           Control.Antisthenis.Minimum
import           Control.Antisthenis.Types
import           Control.Antisthenis.Zipper
import           Control.Antisthenis.ZipperId
import           Control.Arrow                               hiding ((>>>))
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer                        hiding (Sum)
import           Data.Foldable
import qualified Data.List.NonEmpty                          as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Profunctor
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Monoid

type InitRef n = (?initRef :: NodeRef n)

-- | depset has a constant part that is the cost of triggering and a
-- variable part.
data DSetR v p = DSetR { dsetConst :: Sum v,dsetNeigh :: [p] }
  deriving (Functor,Foldable,Traversable)

-- | The dependency set in terms of processes.
type DSet t n p v = DSetR v (NodeProc t n (SumTag p v))

makeCostProc
  :: forall v t n .
  (Invertible v,Ord2 v v,AShow v,InitRef n)
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
    procMin ns = mkProcId mid ns
      where
        mid = zidDefault $ "min:" ++ ashow ref
    procSum :: Int
            -> [NodeProc t n (SumTag (PlanParams n) v)]
            -> NodeProc t n (SumTag (PlanParams n) v)
    procSum i ns = mkProcId mid ns
      where
        mid = zidDefault $ "sum[" ++ show i ++ "]:" ++ ashow ref

type CostParams n v = SumTag (PlanParams n) v
type NoMatProc t n v =
  NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) v) -- The mat proc constructed by the node under consideration
  -> NodeProc t n (SumTag (PlanParams n) v)


-- | Check that a node is materialized and add mark it as such in the
-- coepoch.
ifMaterialized
  :: NodeRef n
  -> NodeProc t n (CostParams n v)
  -> NodeProc t n (CostParams n v)
  -> NodeProc t n (CostParams n v)
ifMaterialized ref t e = squashMealy $ \conf -> do
  let isMater = fromMaybe False $ ref `refLU` peParams (confEpoch conf)
  tell $ mempty { pceParams = refFromAssocs [(ref,isMater)] }
  traceM $ "isMaterialized: " ++ ashow (ref,isMater)
  return (conf,if isMater then t else e)

-- | Build AND INSERT a new mech in the mech directory. The mech does
-- not update it's place in the mech map.α
mkNewMech
  :: forall t n v .
  (Invertible v,Ord2 v v,AShow v,InitRef n)
  => MechConf t n v
  -> NodeRef n
  -> NodeProc t n (CostParams n v)
mkNewMech mc@MechConf {..} ref =
  asSelfUpdating $ ifMaterialized ref (mcIsMatProc ref costProcess) costProcess
  where
    costProcess = squashMealy $ \conf -> do
      mops <- lift2 $ findCostedMetaOps ref
      -- Should never see the same val twice.
      traceM $ "mkNewMech " ++ ashow (ref,fst <$> mops)
      let mechs =
            [DSetR { dsetConst = Sum $ Just $ mcMkCost ref cost
                    ,dsetNeigh =
                       [getOrMakeMech mc n | n <- toNodeList $ metaOpIn mop]
                   } | (mop,cost) <- mops]
      return (conf,makeCostProc ref mechs)
    asSelfUpdating :: NodeProc t n (CostParams n v)
                   -> NodeProc t n (CostParams n v)
    asSelfUpdating (MealyArrow f) = MealyArrow $ fromKleisli $ \c -> do
      ((nxt,r),coepoch) <- listen $ toKleisli f c
      traceM $ "drop-trail:" ++ ashow ref
      lift2
        $ modify
        $ modL mcMechMapLens
        $ refInsert ref
        $ rmap (\x -> trace ("result: " ++ ashow (ref,x,pcePred coepoch)) x)
        $ asSelfUpdating nxt
      return (getOrMakeMech mc ref,r)

markComputable :: NodeRef n -> Conf (CostParams n v) -> Conf (CostParams n v)
markComputable ref conf =
  conf { confEpoch = (confEpoch conf)
           { peCoPred = nsInsert ref $ peCoPred $ confEpoch conf }
       }


-- | Run the machine and see if there are any pradicates. Remember
-- that predicates of a result are a set of nodes that need to be
-- non-compuatable (cycle error) for the result to be valid. As
-- results are solved the predicates are erased by the machines of the
-- predicated nodes. When a predicated node is not computable and is
-- predicated as such it is simply deleted. When a node is predicated
-- as non computable and it turns out to be computable the exact same
-- happens, but the reason is slightly different: due to
-- non-oscillation property (xxx: define it more carefully) while its
-- value turns out to be computable its presence does not change the
-- value itself.
--
-- The property is that assuming a value is not computable while
-- computing the value itself should not affact the final result.
satisfyComputability
  :: (Invertible v,Ord2 v v,AShow v,InitRef n)
  => MechConf t n v
  -> NodeProc t n (CostParams n v)
  -> NodeProc t n (CostParams n v)
satisfyComputability mc c = mkNodeProc $ \conf -> do
  -- first run normally.
  r@(coepoch,(nxt0,_val)) <- runNodeProc c conf
  -- Solve each predicate.
  case toNodeList $ pNonComputables $ pcePred coepoch of
    [] -> return r
    assumedNonComputables -> do
      traceM $ "Checking computability of: " ++ ashow assumedNonComputables
      actuallyComputables <- filterM (isComputableM conf) assumedNonComputables
      let conf' = foldl' (flip markComputable) conf actuallyComputables
      runNodeProc (satisfyComputability mc nxt0) conf'
  where
    mkNodeProc = MealyArrow . WriterArrow . fromKleisli
    runNodeProc = toKleisli . runWriterArrow . runMealyArrow
    isComputableM conf ref = do
      (_coproc,(_nxt,ret))
        <- runNodeProc (satisfyComputability mc $ getOrMakeMech mc ref) conf
      return $ case ret of
        BndErr _ -> False
        _        -> True

markNonComputable :: NodeRef n -> PlanCoEpoch n
markNonComputable
  n = mempty { pcePred = mempty { pNonComputables = nsSingleton n } }
unmarkNonComputable :: NodeRef n -> PlanCoEpoch n -> PlanCoEpoch n
unmarkNonComputable ref pe =
  pe { pcePred = (pcePred pe)
         { pNonComputables = nsDelete ref $ pNonComputables $ pcePred pe }
     }

data MechConf t n v =
  MechConf
  { mcMechMapLens :: GCState t n :>: RefMap n (NodeProc t n (CostParams n v))
   ,mcMkCost      :: NodeRef n -> Cost -> v
   ,mcIsMatProc   :: NoMatProc t n v
   ,mcCompStack   :: NodeRef n -> BndR (CostParams n v)
  }

data a :>:  b = Lens { getL :: a -> b,modL :: (b -> b) -> a -> a }

-- | Bug(?): in the first round we lookup the mech but in subsequent
-- rounds we are not.
getOrMakeMech
  :: (Invertible v,Ord2 v v,AShow v,InitRef n)
  => MechConf t n v
  -> NodeRef n
  -> NodeProc t n (CostParams n v)
getOrMakeMech mc@MechConf {..} ref = squashMealy $ \conf -> do
  traceM $ "lu-ref:" ++ ashow ref
  mechM <- lift2 $ gets $ refLU ref . getL mcMechMapLens
  lift2 $ modify $ modL mcMechMapLens $ refInsert ref $ cycleProc mc ref
  return (conf,censorPredicate ref $ fromMaybe (mkNewMech mc ref) mechM)

-- | Return an error (uncomputable) and insert a predicate that
-- whatever results we come up with are predicated on ref being
-- uncomputable
cycleProc :: MechConf t n v -> NodeRef n -> NodeProc t n (CostParams n v)
cycleProc mc ref =
  arrCoListen'
  $ arr
  $ const (markNonComputable ref,mcCompStack mc ref)

-- | Make sure the predicate of a node being non-computable does not
-- propagate outside of the process. This is useful for wrapping
-- processes that may recurse. If the predicate at NodeRef is indeed
-- non-computable then the predicate holds and we should remove it
-- from the coepoch. If the predicate of ref being non-computable does
-- not hold then the function of assuming non-computability when
-- coming up with said result was to avoid a fixpoint.
--
-- We need a good theoretical foundation on how fixpoints are handled
-- for the particular operators we use.
censorPredicate
  :: NodeRef n
  -> NodeProc t n (CostParams n v)
  -> NodeProc t n (CostParams n v)
censorPredicate ref c = arrCoListen' $ rmap go $ arrListen' c
  where
    go (coepoch,ret) = case ret of
      BndErr ErrCycleEphemeral {}
        -> (unmarkNonComputable ref coepoch
           ,BndErr
              ErrCycle
              { ecCur = ref,ecPred = pNonComputables $ pcePred coepoch })
      _ -> (unmarkNonComputable ref coepoch,ret)

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
getCost mc extraMat cap ref = wrapTrace ("getCost: " ++ ashow ref) $ do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  let extraStates = nsToRef (const True) extraMat
  (res,_coepoch) <- planQuickRun
    $ (`runReaderT` 1) -- scaling
    $ runWriterT
    $ runMech
      (let ?initRef = ref in satisfyComputability mc $ getOrMakeMech mc ref)
    $ Conf
    { confCap = cap
     ,confEpoch = def { peParams = states <> extraStates }
     ,confTrPref = ()
    }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing) -> return $ Just mempty
    BndBnd _bnd -> return Nothing
    BndErr e -> throwPlan $ "getCost:antisthenis error: " ++ ashow e
