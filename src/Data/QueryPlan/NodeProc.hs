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
module Data.QueryPlan.NodeProc (NodeProc,getCostPlan,getCost) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Writer
import           Control.Antisthenis.Convert
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
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.PlanMech
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
type DSet m tag v = DSetR v (ArrProc tag m)

makeCostProc
  :: forall n tag m .
  (Monad m,IsPlanParams (CostParams tag n) n)
  => NodeRef n
  -> [DSet m (CostParams tag n) (MechVal (PlanParams tag n))]
  -> ArrProc (CostParams tag n) m
makeCostProc ref deps =
  convArrProc convMinSum
  $ mkProcId (zidDefault $ "min" <: ref)
  $ zipWith go [1::Int ..] deps
  where
    go i DSetR {..} = convArrProc convSumMin $ p $ constArr : dsetNeigh
      where
        p = mkProcId $ zidDefault $ "sum[" ++ show i ++ "]" <: ref
        constArr = arr $ const $ BndRes dsetConst

-- | Check that a node is materialized and add mark it as such in the
-- coepoch.
ifMaterialized
  :: (Monad m,IsPlanParams (CostParams tag n) n)
  => NodeRef n
  -> ArrProc (CostParams tag n) m
  -> ArrProc (CostParams tag n) m
  -> ArrProc (CostParams tag n) m
ifMaterialized ref t e = proc conf -> do
  let isMater = fromMaybe False $ ref `refLU` peParams (confEpoch conf)
  () <- arrTell -< (mempty { pceParams = refFromAssocs [(ref,isMater)] })
  case isMater of
    True -> t -< conf
    False -> e -< conf

-- | Build AND INSERT a new mech in the mech directory. The mech does
-- not update it's place in the mech map.α
mkNewMech
  :: forall n tag m .
  (PlanMech m (CostParams tag n) n,IsPlanParams (CostParams tag n) n)
  => NodeRef n
  -> ArrProc (CostParams tag n) m
mkNewMech ref =
  asSelfUpdating
  $ ifMaterialized ref (mcIsMatProc @m Proxy ref costProcess) costProcess
  where
    costProcess = squashMealy $ \conf -> do
      -- mops <- lift $ fmap2 (first $ toNodeList . metaOpIn) $ findCostedMetaOps ref
      neigh <- lift $ mcGetNeighbors @m @(CostParams tag n) Proxy ref
      -- ("metaops(" ++ ashow ref ++ ")") <<: mops
      let mechs =
            [DSetR
              { dsetConst =
                  Sum $ Just $ mcMkCost @m @(CostParams tag n) Proxy ref cost
               ,dsetNeigh = [getOrMakeMech n | n <- inp]
              } | (inp,cost) <- neigh]
      return (conf,makeCostProc ref mechs)
    asSelfUpdating :: ArrProc (CostParams tag n) m
                   -> ArrProc (CostParams tag n) m
    asSelfUpdating
      (MealyArrow f) = censorPredicate ref $ MealyArrow $ fromKleisli $ \c -> do
      (nxt,r) <- toKleisli f c
      -- "result" <<: (ref,r)
      lift $ mcPutMech Proxy ref $ asSelfUpdating nxt
      return (getOrMakeMech ref,r)

markComputable
  :: IsPlanParams (CostParams tag n) n
  => NodeRef n
  -> Conf (CostParams tag n)
  -> Conf (CostParams tag n)
markComputable ref conf =
  conf { confEpoch = (confEpoch conf)
           { peCoPred = nsInsert ref $ peCoPred $ confEpoch conf }
       }

setComputables
  :: IsPlanParams (CostParams tag n) n
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
  :: forall m tag n .
  (PlanMech m (CostParams tag n) n,IsPlanParams (CostParams tag n) n)
  => NodeRef n
  -> ArrProc (CostParams tag n) m
  -> ArrProc (CostParams tag n) m
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
          "(comp,non-comp)" <<: (actuallyComputables,assumedNonComputables)
          if null actuallyComputables then return r else do
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
  :: forall tag m n .
  (PlanMech m (CostParams tag n) n,IsPlanParams (CostParams tag n) n)
  => NodeRef n
  -> ArrProc (CostParams tag n) m
getOrMakeMech ref = squashMealy $ \conf -> do
  -- "ref-lu" <<: ref
  mechM <- lift $ mcGetMech @m Proxy ref
  lift $ mcPutMech @m Proxy ref $ cycleProc @tag ref
  return (conf,fromMaybe (mkNewMech ref) mechM)

-- | Return an error (uncomputable) and insert a predicate that
-- whatever results we come up with are predicated on ref being
-- uncomputable.
cycleProc
  :: forall tag n m .
  (PlanMech m (CostParams tag n) n,IsPlanParams (CostParams tag n) n)
  => NodeRef n
  -> ArrProc (CostParams tag n) m
cycleProc ref =
  MealyArrow
  $ WriterArrow
  $ Kleisli
  $ const
  $ return
    (markNonComputable ref
    ,(getOrMakeMech ref,mcCompStackVal @m Proxy ref))

-- | Make sure the predicate of a node being non-computable does not
-- propagate outside of the process. This is useful for wrapping
-- processes that may recurse. If the predicate at NodeRef is indeed
-- non-computable then the predicate holds and we should remove it
-- from the coepoch. If the predicate of ref being non-computable does
-- not hold then the function of assuming non-computability when
-- coming up with said result was to avoid a fixpoint.
--
-- We need a better theoretical foundation on how fixpoints are handled
-- for the particular operators we use.
censorPredicate
  :: (Monad m,IsPlanParams (CostParams tag n) n)
  => NodeRef n
  -> ArrProc (CostParams tag n) m
  -> ArrProc (CostParams tag n) m
censorPredicate ref c =
  MealyArrow
  $ WriterArrow
  $ dimap censorCopred go
  $ runWriterArrow
  $ runMealyArrow c
  where
    -- Copredicates for nodes that are in the computation stack a)
    -- don't make sense and b) will cause the top node to reset as
    -- predicate censoring happens at the outer bound of the process
    -- while copredicates penetrate the outer bound and cause the
    -- process to reset.
    censorCopred conf =
      conf { confEpoch = (confEpoch conf)
               { peCoPred = nsDelete ref $ peCoPred $ confEpoch conf }
           }
    -- Ephemeral errors are removed as soon as possible, they are
    -- emitted at the PlanMech level but we handle them here.
    go (coepoch,(nxt,ret)) = case ret of
      BndErr ErrCycleEphemeral {} ->
        (unmarkNonComputable ref coepoch
        ,(nxt
         ,BndErr
            ErrCycle { ecCur = ref,ecPred = pNonComputables $ pcePred coepoch }))
      _ -> (unmarkNonComputable ref coepoch,(nxt,ret))

getCostPlan
  :: forall tag t n m .
  (PlanMech (PlanT t n Identity) (CostParams tag n) n
  ,HasCallStack
  ,IsPlanParams (CostParams tag n) n
  ,AShow (MechVal (PlanParams tag n))
  ,Monad m)
  => Proxy tag
  -> NodeSet n
  -> Cap (ExtCap (PlanParams tag n))
  -> NodeRef n
  -> PlanT t n m (Maybe (MechVal (PlanParams tag n)))
getCostPlan Proxy extraMat cap ref = do
  states0 <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  let states =
        foldl' (\st n -> refInsert n True st) states0 $ toNodeList extraMat
  st0 <- get
  conf <- ask
  case runIdentity
    $ runExceptT
    $ (`runReaderT` conf)
    $ (`runStateT` st0)
    $ getCost @tag Proxy cap states ref of
      Left e       -> throwError e
      Right (a,st) -> put st >> return a

getCost
  :: forall tag n m .
  (PlanMech m (CostParams tag n) n
  ,HasCallStack
  ,IsPlanParams (CostParams tag n) n
  ,AShow (MechVal (PlanParams tag n)))
  => Proxy tag
  -> Cap (ExtCap (PlanParams tag n))
  -> RefMap n Bool
  -> NodeRef n
  -> m (Maybe (MechVal (PlanParams tag n)))
getCost _ cap states ref = wrapTr $ do
  (res,_coepoch) <- runWriterT
    $ runMech (satisfyComputability @m @tag ref $ getOrMakeMech ref)
    $ Conf
    { confCap = cap,confEpoch = def { peParams = states },confTrPref = () }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing) -> return $ Just zero
    BndBnd _bnd -> return Nothing
    BndErr e ->
      error $ "getCost(" ++ ashow ref ++ "):antisthenis error: " ++ ashow e
  where
    -- wrapTr = wrapTrace ("getCost" <: ref)
    wrapTr = id
