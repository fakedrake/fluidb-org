{-# LANGUAGE Arrows                #-}
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
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Data.QueryPlan.NodeProc (NodeProc,getCost,(:>:)(..)) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Writer
import           Control.Antisthenis.Convert
import           Control.Antisthenis.Lens
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
import           Data.Proxy
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Nat

-- | depset has a constant part that is the cost of triggering and a
-- variable part.
data DSetR v p = DSetR { dsetConst :: Sum v,dsetNeigh :: [p] }
  deriving (Functor,Foldable,Traversable)

-- | The dependency set in terms of processes.
type DSet t n p v = DSetR v (NodeProc t n (SumTag p))

makeCostProc
  :: forall t n tag .
  IsPlanParams tag n
  => NodeRef n
  -> [DSet t n (PlanParams tag n) (MechVal (PlanParams tag n))]
  -> NodeProc t n (SumTag (PlanParams tag n))
makeCostProc ref deps =
  convArrProc convMinSum $ procMin $ zipWith go [1 ..] deps
  where
    go i DSetR {..} = convArrProc convSumMin $ procSum i $ constArr : dsetNeigh
      where
        constArr = arr $ const $ BndRes dsetConst
    procMin :: (mintag ~ MinTag (PlanParams tag n)
               ,NodeProc0 t n (CostParams tag n) mintag ~ p)
            => [p]
            -> p
    procMin ns = mkProcId mid ns
      where
        mid = zidDefault $ "min:" ++ ashow ref
    procSum :: Int
            -> [NodeProc t n (CostParams tag n)]
            -> NodeProc t n (CostParams tag n)
    procSum i ns = mkProcId mid ns
      where
        mid = zidDefault $ "sum[" ++ show i ++ "]:" ++ ashow ref

-- | Check that a node is materialized and add mark it as such in the
-- coepoch.
ifMaterialized
  :: IsPlanParams tag n
  => NodeRef n
  -> NodeProc t n (CostParams tag n)
  -> NodeProc t n (CostParams tag n)
  -> NodeProc t n (CostParams tag n)
ifMaterialized ref t e = proc conf -> do
  let isMater = fromMaybe False $ ref `refLU` peParams (confEpoch conf)
  () <- arrTell -< (mempty { pceParams = refFromAssocs [(ref,isMater)] })
  case isMater of
    True -> t -< conf
    False -> e -< conf

-- | Build AND INSERT a new mech in the mech directory. The mech does
-- not update it's place in the mech map.α
mkNewMech
  :: forall t n tag .
  IsPlanParams tag n
  => NodeRef n
  -> NodeProc t n (CostParams tag n)
mkNewMech ref =
  asSelfUpdating $ ifMaterialized ref (mcIsMatProc ref costProcess) costProcess
  where
    costProcess = squashMealy $ \conf -> do
      mops <- lift $ findCostedMetaOps ref
      -- ("metaops("++ ashow ref ++ ")") <<: mops
      -- Should never see the same val twice.
      let mechs =
            [DSetR
              { dsetConst = Sum $ Just $ mcMkCost (Proxy :: Proxy tag) ref cost
               ,dsetNeigh = [getOrMakeMech n | n <- toNodeList $ metaOpIn mop]
              } | (mop,cost) <- mops]
      return (conf,makeCostProc ref mechs)
    asSelfUpdating :: NodeProc t n (CostParams tag n)
                   -> NodeProc t n (CostParams tag n)
    asSelfUpdating
      (MealyArrow f) = censorPredicate ref $ MealyArrow $ fromKleisli $ \c -> do
      (nxt,r) <- toKleisli f c
      -- ("result(" ++ ashow ref ++ ")") <<: r
      lift $ modify $ modL mcMechMapLens $ refInsert ref $ asSelfUpdating nxt
      return (getOrMakeMech ref,r)

markComputable
  :: IsPlanParams tag n
  => NodeRef n
  -> Conf (CostParams tag n)
  -> Conf (CostParams tag n)
markComputable ref conf =
  conf { confEpoch = (confEpoch conf)
           { peCoPred = nsInsert ref $ peCoPred $ confEpoch conf }
       }

setComputables
  :: IsPlanParams tag n
  => NodeSet n
  -> Conf (CostParams tag n)
  -> Conf (CostParams tag n)
setComputables refs conf =
  conf { confEpoch = (confEpoch conf) { peCoPred = refs } }

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
  :: IsPlanParams tag n
  => NodeRef n
  -> NodeProc t n (CostParams tag n)
  -> NodeProc t n (CostParams tag n)
satisfyComputability = go mempty
  where
    mkNodeProc = MealyArrow . WriterArrow . fromKleisli
    runNodeProc = toKleisli . runWriterArrow . runMealyArrow
    go trail ref c = mkNodeProc $ \conf -> do
      -- first run normally.
      r@(coepoch,(nxt0,_val)) <- runNodeProc c conf
      -- Solve each predicate.
      case toNodeList $ pNonComputables $ pcePred coepoch of
        [] -> return r
        assumedNonComputables -> do
          actuallyComputables
            <- filterM (isComputableM conf) assumedNonComputables
          let conf' = foldl' (flip markComputable) conf actuallyComputables
          runNodeProc (go trail ref nxt0) conf'
      where
        isComputableM conf ref' = do
          (_coproc,(_nxt,ret)) <- runNodeProc
            (go (trail <> nsSingleton ref) ref' $ getOrMakeMech ref')
            $ setComputables trail conf
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

getOrMakeMech
  :: forall tag t n .
  IsPlanParams tag n
  => NodeRef n
  -> NodeProc t n (CostParams tag n)
getOrMakeMech ref = squashMealy $ \conf -> do
  -- "ref-lu" <<: ref
  mechM <- lift $ gets $ refLU ref . getL mcMechMapLens
  lift
    $ modify
    $ modL mcMechMapLens
    $ refInsert ref
    $ cycleProc @tag ref
  return (conf,fromMaybe (mkNewMech ref) mechM)

-- | Return an error (uncomputable) and insert a predicate that
-- whatever results we come up with are predicated on ref being
-- uncomputable.
cycleProc :: IsPlanParams tag n => NodeRef n -> NodeProc t n (CostParams tag n)
cycleProc ref =
  MealyArrow $ rmap (first $ const $ getOrMakeMech ref) $ runMealyArrow go
  where
    go = arrCoListen' $ arr $ const (markNonComputable ref,mcCompStackVal ref)

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
  :: IsPlanParams tag n
  => NodeRef n
  -> NodeProc t n (CostParams tag n)
  -> NodeProc t n (CostParams tag n)
censorPredicate ref c = arrCoListen' $ rmap go $ arrListen' c
  where
    go (coepoch,ret) = case ret of
      BndErr ErrCycleEphemeral {} ->
        (unmarkNonComputable ref coepoch
        ,BndErr
           ErrCycle { ecCur = ref,ecPred = pNonComputables $ pcePred coepoch })
      _ -> (unmarkNonComputable ref coepoch,ret)

planQuickRun :: Monad m => PlanT t n Identity a -> PlanT t n m a
planQuickRun m = do
  st0 <- get
  conf <- ask
  case runIdentity $ runExceptT $ (`runReaderT` conf) $ (`runStateT` st0) m of
    Left e       -> throwError e
    Right (a,st) -> put st >> return a

getCost
  :: forall tag t n m .
  (Monad m,IsPlanParams tag n,AShow (MechVal (PlanParams tag n)))
  => Proxy tag
  -> NodeSet n
  -> Cap (ExtCap (PlanParams tag n))
  -> NodeRef n
  -> PlanT t n m (Maybe (MechVal (PlanParams tag n)))
getCost _ extraMat cap ref = wrapTrace ("getCost" <: ref) $ do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  let extraStates = nsToRef (const True) extraMat
  (res,_coepoch) <- planQuickRun
    $ runWriterT
    $ runMech (satisfyComputability @tag ref $ getOrMakeMech ref)
    $ Conf
    { confCap = cap
     ,confEpoch = def { peParams = states <> extraStates }
     ,confTrPref = ()
    }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing) -> return $ Just zero
    BndBnd _bnd -> return Nothing
    BndErr e -> throwPlan $ "getCost:antisthenis error: " ++ ashow e
