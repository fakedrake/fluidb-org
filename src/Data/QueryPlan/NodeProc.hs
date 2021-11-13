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
module Data.QueryPlan.NodeProc (NodeProc,getPlanBndR) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Writer
import           Control.Antisthenis.Types
import           Control.Antisthenis.Zipper
import           Control.Applicative
import           Control.Arrow                               hiding ((>>>))
import           Control.Monad.Except
import           Control.Monad.Extra
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
import           Data.Utils.Functors
import           Data.Utils.ListT
import           Data.Utils.Unsafe

-- | Check that a node is materialized and add mark it as such in the
-- coepoch.
ifMaterialized
  :: forall m w n .
  (Monad m,IsPlanParams w n,PlanMech m w n)
  => NodeRef n
  -> ArrProc w m
  -> ArrProc w m
  -> ArrProc w m
ifMaterialized ref t e = squashMealy $ \conf -> do
  let isMater = fromMaybe False $ ref `refLU` peParams (confEpoch conf)
  when (mcShouldTell @m @w @n Proxy ref isMater) $
    tell (mempty { pceParams = refFromAssocs [(ref,isMater)] })
  return (conf,if isMater then t else e)

-- proc conf -> do
-- let isMater = fromMaybe False $ ref `refLU` peParams (confEpoch conf)
-- () <- arrTell -< (mempty { pceParams = refFromAssocs [(ref,isMater)] })
-- case isMater of
--   True -> t -< conf
--   False -> e -< conf

-- | Build AND INSERT a new mech in the mech directory. The mech does
-- not update it's place in the mech map.α

mkNewMech
  :: forall w n  m .
  (PlanMech m w n,IsPlanParams w n)
  => NodeRef n
  -> ArrProc w m
mkNewMech ref =
  asSelfUpdating
  $ ifMaterialized ref (mcIsMatProc @m Proxy ref newProcess) newProcess
  where
    newProcess :: ArrProc w m
    newProcess = mcMkProcess getOrMakeMech ref
    -- costProcess = squashMealy $ \conf -> do
    --   -- mops <- lift $ fmap2 (first $ toNodeList . metaOpIn) $ findCostedMetaOps ref
    --   neigh <- lift $ mcGetNeighbors @m @p Proxy ref
    --   -- ("metaops(" ++ ashow ref ++ ")") <<: mops
    --   let mechs =
    --         [DSetR
    --           { dsetConst =
    --               Sum $ Just $ mcMkCost @m @p Proxy ref cost
    --            ,dsetNeigh = [getOrMakeMech n | n <- inp]
    --           } | (inp,cost) <- neigh]
    --   return (conf,makeCostProc ref mechs)
    asSelfUpdating :: ArrProc w m -> ArrProc w m
    asSelfUpdating
      (MealyArrow f) = censorPredicate ref $ MealyArrow $ fromKleisli $ \c -> do
      ((nxt,r),_co) <- listen $ toKleisli f c
      -- XXX: the copred and pred do not match so the loop is not due
      -- to the epoch/coepoch.  We are also NOT resetting. These
      -- repeating nodes are NOT in the same cycle (otherwise they
      -- would not be returning).
      --
      -- It is a histcost! The cap increases and the bounds also increase
      -- "result" <<: (ref,confCap c,r)
      lift $ mcPutMech Proxy ref $ asSelfUpdating nxt
      return (getOrMakeMech ref,r)

markComputable
  :: IsPlanParams w n
  => NodeRef n
  -> Conf w
  -> Conf w
markComputable ref conf =
  conf { confEpoch = (confEpoch conf)
           { peCoPred = nsInsert ref $ peCoPred $ confEpoch conf }
       }
unmarkComputable :: IsPlanParams w n => NodeRef n -> Conf w -> Conf w
unmarkComputable ref conf =
  conf { confEpoch = (confEpoch conf)
           { peCoPred = nsDelete ref $ peCoPred $ confEpoch conf }
       }

setComputables :: IsPlanParams w n => NodeSet n -> Conf w -> Conf w
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
data CompTrail n = CompTrail { ctIsComp :: NodeSet n,ctNotComp :: NodeSet n }
type CompT n m = ReaderT (NodeSet n) (StateT (CompTrail n) (ListT m))
runCompT :: Monad m => CompT n m a -> m a
runCompT m =
  fmap fromJustErr
  $ headListT
  $ (`evalStateT` CompTrail mempty mempty)
  $ (`runReaderT` mempty) m

satisfyComputability
  :: forall m w n .
  (PlanMech m w n,IsPlanParams w n)
  => NodeRef n
  -> ArrProc w m
  -> Conf w
  -> m (BndR w)
satisfyComputability ref0 mech conf = runCompT $ go ref0 mech
  where
    runNodeProc
      :: ArrProc w m -> CompT n m (PlanCoEpoch n,(ArrProc w m,BndR w))
    runNodeProc c = do
      ct <- get
      lift3
        $ toKleisli (runWriterArrow $ runMealyArrow c)
        $ setComputables (ctIsComp ct) conf
    isComp0 = mcIsComputable @m @w @n Proxy
    regRef ref val = if isComp0 val then setComp else setNoComp
      where
        setComp = modify $ \ct -> ct { ctIsComp = nsInsert ref $ ctIsComp ct }
        setNoComp =
          modify $ \ct -> ct { ctNotComp = nsInsert ref $ ctNotComp ct }
    unlessEncountered :: NodeRef n -> CompT n m Bool -> CompT n m Bool
    unlessEncountered ref m = do
      CompTrail {..} <- get
      case (ref `nsMember` ctIsComp,ref `nsMember` ctNotComp) of
        (True,False) -> return True
        (True,True) -> error "oops"
        (False,True) -> return False
        (False,False) -> do
          trail <- ask
          if ref `nsMember` trail then return False <|> return True else m
    go :: NodeRef n -> ArrProc w m -> CompT n m (BndR w)
    go ref c = do
      "evaluating" <<: ref
      -- first run normally.
      (coepoch,(nxt0,val)) <- runNodeProc c
      -- Solve each predicate.
      case toNodeList $ pNonComputables $ pcePred coepoch of
        [] -> do
          regRef ref val
          return val
        assumedNonComputables -> do
          anyComputable <- anyM isComputableM assumedNonComputables
          if anyComputable then go ref nxt0 else return val
      where
        isComputableM :: NodeRef n -> CompT n m Bool
        isComputableM ref' = unlessEncountered ref' $ do
          ret <- local (nsInsert ref') $ go ref' $ getOrMakeMech ref'
          return $ isComp0 ret


markNonComputable :: NodeRef n -> PlanCoEpoch n
markNonComputable
  n = mempty { pcePred = mempty { pNonComputables = nsSingleton n } }
unmarkNonComputable :: NodeRef n -> PlanCoEpoch n -> PlanCoEpoch n
unmarkNonComputable ref pe =
  pe { pcePred = (pcePred pe)
         { pNonComputables = nsDelete ref $ pNonComputables $ pcePred pe }
     }

getOrMakeMech
  :: forall m n w .
  (PlanMech m w n,IsPlanParams w n)
  => NodeRef n
  -> ArrProc w m
getOrMakeMech ref = squashMealy $ \conf -> do
  mechM :: Maybe (ArrProc w m)
    <- lift $ mcGetMech @m Proxy ref
  let conf' = unmarkComputable ref conf
  lift $ mcPutMech @m Proxy ref $ cycleProc @w ref
  return (conf',fromMaybe (mkNewMech ref) mechM)

-- | Return an error (uncomputable) and insert a predicate that
-- whatever results we come up with are predicated on ref being
-- uncomputable.
cycleProc
  :: forall w n m .
  (PlanMech m w n,IsPlanParams w n)
  => NodeRef n
  -> ArrProc w m
cycleProc ref =
  MealyArrow
  $ WriterArrow
  $ Kleisli
  $ const
  $ return
    (markNonComputable ref,(getOrMakeMech ref,mcCompStackVal @m Proxy ref))

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
  :: (Monad m,IsPlanParams w n)
  => NodeRef n
  -> ArrProc w m
  -> ArrProc w m
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

-- | Cap is used by History to make sure we dont go all the way down
-- the rabbit hole.
getPlanBndR
  :: forall w t n m .
  (PlanMech (PlanT t n Identity) w n
  ,HasCallStack
  ,ZCap w ~ ExtCap (MetaTag w)
  ,IsPlanParams w n
  ,Monad m)
  => Proxy w
  -> Cap (ExtCap (MetaTag w))
  -> NodeRef n
  -> PlanT t n m (BndR w)
getPlanBndR Proxy cap ref = do
  states <- gets $ fmap isMat . nodeStates . NEL.head . epochs
  st0 <- get
  conf <- ask
  case runIdentity
    $ runExceptT
    $ (`runReaderT` conf)
    $ (`runStateT` st0)
    $ getBndR @w Proxy cap states ref of
      Left e       -> throwError e
      Right (a,st) -> put st >> return a

getBndR
  :: forall w n m .
  (PlanMech m w n
  ,HasCallStack
  ,IsPlanParams w n
  ,ZCap w ~ ExtCap (MetaTag w))
  => Proxy w
  -> Cap (ExtCap (MetaTag w))
  -> RefMap n Bool
  -> NodeRef n
  -> m (BndR w) -- (Maybe (MechVal w))
getBndR _ cap states ref = wrapTr $ do
  res <- satisfyComputability @m @w ref (getOrMakeMech ref)
    $ Conf
    { confCap = cap,confEpoch = def { peParams = states },confTrPref = () }
  return res
  where
    -- case res of
    --   BndRes (Sum (Just r)) -> return $ Just r
    --   BndRes (Sum Nothing) -> return $ Just zero
    --   BndBnd _bnd -> return Nothing
    --   BndErr e ->
    --     error $ "getCost(" ++ ashow ref ++ "):antisthenis error: " ++ ashow e
    -- wrapTr = wrapTrace ("getBndR" <: ref)
    wrapTr = id
