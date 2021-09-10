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
import           Control.Antisthenis.ATL.Common
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
import           Data.NodeContainers
import           Data.Profunctor
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.ProcTrail
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
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

-- | Build AND INSERT a new mech in the mech directory. The mech does
-- not update it's place in the mech map.
mkNewMech
  :: forall t n v .
  (Invertible v,Ord2 v v,AShow v,InitRef n)
  => MechConf t n v
  -> NodeRef n
  -> NodeProc t n (CostParams n v)
mkNewMech mc@MechConf {..} ref = squashMealy $ do
  mops <- lift2 $ findCostedMetaOps ref
  -- Should never see the same val twice.
  traceM $ "mkNewMech " ++ ashow (ref,fst <$> mops)
  let mechs =
        [DSetR
          { dsetConst = Sum $ Just $ mcMkCost ref cost
           ,dsetNeigh = [getOrMakeMech mc n | n <- toNodeList $ metaOpIn mop]
          } | (mop,cost) <- mops]
  let costProcess = makeCostProc ref mechs
  return
    $ asSelfUpdating
    $ mkEpoch id ref >>> mcIsMatProc ref costProcess ||| costProcess
  where
    asSelfUpdating :: NodeProc t n (CostParams n v)
                   -> NodeProc t n (CostParams n v)
    asSelfUpdating (MealyArrow f) = MealyArrow $ fromKleisli $ \c -> do
      (nxt,r) <- toKleisli f c
      traceM $ "drop-trail:" ++ ashow ref
      lift2
        $ modify
        $ modL mcMechMapLens
        $ refInsert ref
        $ rmap (\x -> trace ("result: " ++ ashow (ref,x)) x)
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
  :: MechConf t n v
  -> NodeProc t n (CostParams n v)
  -> NodeProc t n (CostParams n v)
satisfyComputability mc c = mkNodeProc $ \conf -> do
  -- first run normally.
  r@(coepoch,(nxt0,_val)) <- runNodeProc c conf
  -- Solve each predicate.
  case toNodeList $ pData $ pcePred coepoch of
    [] -> return r
    assumedNonComputables -> do
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

isPredicated :: NodeRef n -> ZCoEpoch (CostParams n v) -> Bool
isPredicated ref coepoch = nsMember ref $ pData $ pcePred coepoch
markNonComputable :: NodeRef n -> Predicates n
markNonComputable n = mempty { pDrop = [n] }
assumeNonComputable :: NodeRef n -> Predicates n
assumeNonComputable n = mempty { pData = nsSingleton n }

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
  :: (Invertible v,Ord2 v v,AShow v,InitRef n)
  => MechConf t n v
  -> NodeRef n
  -> NodeProc t n (SumTag (PlanParams n) v)
getOrMakeMech mc@MechConf {..} ref = squashMealy $ do
  traceM $ "lu-ref:" ++ ashow ref
  lift2 (gets $ refLU ref . getL mcMechMapLens) >>= \case
    Nothing -> do
      lift2 $ modify $ modL mcMechMapLens $ refInsert ref $ cycleProc ref
      return $ mkNewMech mc ref
    Just x -> do
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
      traceM $ "put-trail1:" ++ ashow ref
      lift2 $ modify $ modL mcMechMapLens $ refInsert ref $ cycleProc ref
      return x


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
    $ Conf { confCap = cap,confEpoch = states <> extraStates,confTrPref = () }
  case res of
    BndRes (Sum (Just r)) -> return $ Just r
    BndRes (Sum Nothing) -> return $ Just mempty
    BndBnd _bnd -> return Nothing
    BndErr e -> throwPlan $ "getCost:antisthenis error: " ++ ashow e
