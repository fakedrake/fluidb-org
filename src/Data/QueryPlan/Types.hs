{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# OPTIONS_GHC -Wno-orphans -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.QueryPlan.Types
  (Transition(..)
  ,HistTag
  ,CostTag
  ,HistProc
  ,CostProc
  ,PCost
  ,IsPlanParams
  ,PlanMech(..)
  ,CostParams
  ,NoMatProc
  ,QueryHistory(..)
  ,PlanT
  ,IsMatable
  ,PlanSearchScore(..)
  ,MonadHaltD
  ,MetaOp(..)
  ,Predicates(..)
  ,CoPredicates
  ,PlanEpoch(..)
  ,PlanCoEpoch(..)
  ,pushHistory
  ,showMetaOp
  ,throwPlan
  ,halt
  ,GCState(..)
  ,GCConfig(..)
  ,GCEpoch(..)
  ,NodeState(..)
  ,PlanningError(..)
  ,IsMat(..)
  ,Cost(..)
  ,BotMonad(..)
  ,PlanSanityError(..)
  ,ProvenanceAtom(..)
  ,MonadLogic
  ,hoistPlanT
  ,costAsInt
  ,lowerNodeProc
  ,liftNodeProc
  ,Count
  ,NodeProc
  ,PlanParams
  ,SumTag
  ,NodeProc0
  ,bot
  ,top
  ,runPlanT'
  ,liftPlanT
  ,trM
  ,mplusPlanT) where


import           Control.Antisthenis.Bool
import           Control.Antisthenis.Convert
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Trans.Maybe
import           Control.Monad.Writer        hiding (Sum)
import           Data.Bipartite
import           Data.Function
import           Data.Group
import qualified Data.HashSet                as HS
import           Data.IntMap                 (Key)
import           Data.List
import qualified Data.List.NonEmpty          as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Proxy
import           Data.Query.QuerySize
import           Data.QueryPlan.Comp
import           Data.QueryPlan.CostTypes
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.HContT
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Nat
import           GHC.Generics                (Generic)
import           GHC.Stack
import           Prelude                     hiding (filter, lookup)

data IsMat = Mat | NoMat deriving (Show, Read, Eq, Generic)
data NodeState =
  -- Concretified states (what it was, what it became)
  Concrete IsMat IsMat
  -- Initial states are subject to change
  | Initial IsMat
  deriving (Show, Read, Eq, Generic)

instance AShow NodeState
instance ARead NodeState
instance AShow IsMat
instance ARead IsMat

-- | Transition actions. Interpretation of each transition action
-- should be relative to the nodeStates map. Each node is materialized
-- only by the *first* occuring trigger.
data Transition t n
  = Trigger [NodeRef n] (NodeRef t) [NodeRef n]
  | RTrigger [NodeRef n] (NodeRef t) [NodeRef n]
  | DelNode (NodeRef n)
  deriving (Show, Eq, Generic)

instance AShow (Transition t n)

data GCEpoch t n = GCEpoch {
  nodeStates  :: RefMap n NodeState,
  transitions :: [Transition t n]
  } deriving (Show, Eq, Generic)

instance Hashable IsMat
instance Hashable NodeState
instance Hashable (Transition t n)
instance Hashable (GCEpoch t n)

type Count = Int
data ProvenanceAtom
  = EitherLeft
  | EitherRight
  | SplitLeft
  | SplitRight
  deriving Show
provenanceAsBool :: ProvenanceAtom -> Bool
provenanceAsBool = \case
  EitherLeft  -> False
  SplitLeft   -> False
  EitherRight -> True
  SplitRight  -> True

type PCost = Comp Cost
type IsMatable = Bool
type CostProc t n = NodeProc t n (CostParams CostTag n)
type HistProc t n = NodeProc t n (CostParams HistTag n)
type MatProc t n = NodeProc t n (BoolTag Or (PlanParams CostTag n))
data GCState t n =
  GCState
  { frontier          :: NodeSet n
   ,gcMechMap         :: RefMap n (CostProc t n)
   ,gcHistMechMap     :: RefMap n (HistProc t n)
   ,matableMechMap    :: RefMap n (MatProc t n)
   ,gcCache           :: GCCache (MetaOp t n) t n
   ,epochs            :: NEL.NonEmpty (GCEpoch t n)
    -- | Nodes with nonzero protection are not demoted when the epoch changes
   ,nodeProtection    :: RefMap n Count
   ,epochFilter       :: HS.HashSet (GCEpoch t n,RefMap n Count)
   ,provenance        :: [ProvenanceAtom]
   ,traceDebug        :: [String]
    -- aStarScores       :: RefMap n Double,
   ,garbageCollecting :: Bool
  }
  deriving Generic

type Certainty = Double
newtype QueryHistory n =
  QueryHistory { unQueryHistory :: [NodeRef n] }
  deriving Generic
instance Default (QueryHistory n)
data GCConfig t n =
  GCConfig
  { -- These are configuration but ok.
    propNet       :: Bipartite t n
   ,nodeSizes     :: RefMap n ([TableSize],Certainty)
   ,intermediates :: NodeSet n
   ,budget        :: Maybe PageNum
   ,queryHistory  :: QueryHistory n
   ,maxBranching  :: Maybe Count
   ,maxTreeDepth  :: Maybe Count
  }
  deriving Generic
pushHistory :: NodeRef n -> GCConfig t n -> GCConfig t n
pushHistory n conf =
  conf
  { queryHistory = QueryHistory $ (n :) $ unQueryHistory $ queryHistory conf
  }
instance Default (GCConfig t n)
instance Default (GCEpoch t n)
instance Default (GCState t n)

trM :: Monad m => String -> PlanT t n m ()
trM = const $ return ()
{-# INLINE trM #-}


traceMsg :: Monad m => String -> PlanT t n m String
traceMsg = branchingIdTrace

branchingIdTrace :: MonadState (GCState t n) m => String -> m String
branchingIdTrace msg = do
  st <- get
  return $ printf "[branch:%s] %s" (
    show
    $ reverse
    $ provenance st)
    msg


-- | Without the continuation monad, whenever a sub chain of monads is
-- evaluated (eg `a >> b >> c` in `(a >> b >> c) >> d`), all it's
-- branches are searched before moving on to the next monad. With a
-- continuation, instead of evaluating we keep track of the next block
-- until the final `runContT`.
--
-- newtype PlanDebugList t n x = PlanDebugList {
--   unPlanDebugList :: forall r . ListT (ContT r (State [DebugTrace t n])) x
--   } deriving Functor


-- | A monad that keeps track of logic (MonadPlus), of a state of each
-- node (IntMap NodeState)
data PlanningError t n = LookupFail String (NodeRef n) [NodeRef n]
  | TriggerNonMat (Transition t n) [NodeRef n]
  | NonExistentNode (Either (NodeRef t) (NodeRef n))
  | NegativeProtection Key
  | RecordLargerThanPageSize PageSize TableSize (NodeRef n)
  -- We expect that all node refs in an empty
  -- should have a size.
  | NoSizeForBottom (NodeRef n)
  | PlanningErrorMsg String
  deriving (Eq, Show,Generic)
instance AShow (PlanningError t n)
instance IsString (PlanningError t n) where fromString = PlanningErrorMsg
throwPlan :: (MonadError (PlanningError t n) m,HasCallStack) => String -> m a
throwPlan msg = throwError $ fromString
  $ prettyCallStack callStack ++ "\n--\n" ++ msg
class Monad m => BotMonad m  where
  -- | If the branch with the left succeeds don't proceed. If it fails
  -- proceed.
  eitherl' :: m a -> m a -> m a

-- XXX: Watch out: We need the scemantics that come from ContT
instance (Partitionable m, Ord v,Group v,BotMonad m) =>
         BotMonad (HContT v r m) where
  eitherl' = eitherlHCont
instance BotMonad [] where
  eitherl' [] a = a
  eitherl' a _  = a
instance Monad m => BotMonad (MaybeT m) where
  eitherl' (MaybeT xM) (MaybeT yM) = MaybeT $ eitherl' <$> xM <*> yM
instance Monad m => BotMonad (ListT m) where
  eitherl'  = eitherlListT
instance BotMonad Maybe where
  eitherl' Nothing a = a
  eitherl' a _       = a
instance BotMonad Identity where
  eitherl' x _ = x
instance BotMonad m => BotMonad (StateT s m) where
  eitherl' (StateT f) (StateT g) = StateT $ \s -> f s `eitherl'` g s
instance BotMonad m => BotMonad (ReaderT r m) where
  eitherl' (ReaderT f) (ReaderT g) = ReaderT $ \r -> f r `eitherl'` g r
instance BotMonad m => BotMonad (ExceptT e m) where
  eitherl' (ExceptT a) (ExceptT b) = ExceptT $ a `eitherl'` b

-- | This was a bad path, clear all our progress
bot :: MonadPlus m => String -> PlanT t n m a
bot msg = do
  get >>= (\case
    x:_ -> trM x
    _   -> return ()) . traceDebug
  trM $ "BOT: " ++ msg
  lift3 mzero

-- | Just stop.
top :: MonadPlus m => PlanT t n m ()
top = return ()

-- |Incrment the tree depth counter.
incTreeProp :: MonadPlus m =>
              String
            -> PlanT t n m Int
            -> PlanT t n m (Maybe Int)
            -> PlanT t n m ()
            -> PlanT t n m ()
incTreeProp label getProp getMaxProp modProp  = do
  br <- getProp
  maxBr <- getMaxProp
  lift3 $ guard $ maybe True (br <) maxBr
  modProp
  newBr <- getProp
  when (newBr < 0) $ error $ "sub zero " ++ label

data PlanSanityError t n
  = MissingScore (NodeRef n) (RefMap n Double)
  | MissingSize (NodeRef n) (RefMap n [TableSize])
  | NoError
  deriving (Eq, Show, Generic)
instance AShow (PlanSanityError t n)
data PlanSearchScore = PlanSearchScore {
  planSeachScore      :: Double, -- The cost of the current
  planSearchScoreStar :: Maybe Double
  } deriving (Generic,Show,Eq)
instance Ord PlanSearchScore where
  compare l r = toI l `compareM` toI r where
    compareM Nothing Nothing   = EQ
    compareM Nothing _         = GT
    compareM _ Nothing         = LT
    compareM (Just x) (Just y) = compare x y
    toI PlanSearchScore{..} = (planSeachScore +) <$> planSearchScoreStar
  {-# INLINE compare #-}
instance Monoid PlanSearchScore where
  {-# INLINE mempty #-}
  mempty = PlanSearchScore  {
    planSeachScore=0,
    planSearchScoreStar=Nothing
    }
instance Group PlanSearchScore where
  invert l = l{planSeachScore = -(planSeachScore l)}
instance Semigroup PlanSearchScore where
  {-# INLINE (<>) #-}
  l <> r = PlanSearchScore{
    planSeachScore=planSeachScore l + planSeachScore r,
    planSearchScoreStar=planSearchScoreStar r <|> planSearchScoreStar l
    }
type MonadLogic m = (MonadPlus m, BotMonad m, MonadHaltD m)
type MonadHaltD m = (HValue m ~ PlanSearchScore, MonadHalt m)
type PlanT t n m =
  StateT (GCState t n) (ReaderT (GCConfig t n) (ExceptT (PlanningError t n) m))

hoistPlanT
  :: (m (Either (PlanningError t n) (a,GCState t n))
      -> g (Either (PlanningError t n) (a,GCState t n)))
  -> PlanT t n m a
  -> PlanT t n g a
hoistPlanT f = hoistStateT $ hoistReaderT $ hoistExceptT f
  where
    hoistStateT :: (m (a, s) -> g (a, s)) -> StateT s m a -> StateT s g a
    hoistStateT f (StateT m) = StateT $ \s -> f $ m s
    hoistReaderT :: (m a -> g a) -> ReaderT s m a -> ReaderT s g a
    hoistReaderT f (ReaderT m) = ReaderT $ \r -> f $ m r
    hoistExceptT ::
         (m (Either e a) -> g (Either e a)) -> ExceptT e m a -> ExceptT e g a
    hoistExceptT f (ExceptT m) = ExceptT $ f m

liftPlanT :: Monad m => m a -> PlanT t n m a
liftPlanT = lift3
{-# INLINE liftPlanT #-}
runPlanT' ::
     Monad m
  => GCState t n
  -> GCConfig t n
  -> PlanT t n m a
  -> m (Either (PlanningError t n) a)
runPlanT' gcs conf = runExceptT
                    . (`runReaderT` conf)
                    . (`evalStateT` gcs)
mplusPlanT :: MonadPlus m => PlanT t n m a -> PlanT t n m a -> PlanT t n m a
mplusPlanT = mplusPlanT'
{-# INLINE mplusPlanT #-}


-- | For epoch
isMoreRecent :: Ord v => RefMap n v -> RefMap n v -> Bool
isMoreRecent delta = or . refIntersectionWithKey (const (>=)) delta

mplusPlanT' :: MonadPlus m =>
              StateT s (ReaderT r (ExceptT e m)) a
            -> StateT s (ReaderT r (ExceptT e m)) a
            -> StateT s (ReaderT r (ExceptT e m)) a
mplusPlanT' s s' = s `mplusState` s'
  where
    mplusState (StateT f) (StateT f') = StateT $ \s -> f s `mplusReader` f' s
      where
        mplusReader (ReaderT v) (ReaderT v') =
          ReaderT $ \r -> v r `mplusExcept` v' r
          where
            mplusExcept (ExceptT v) (ExceptT v') = ExceptT $ v `mplus` v'


-- METAOP

-- | A set of triggers that may have Intermediate nodes on the output
-- side but never on the input side. XXX fix the following code.
data MetaOp t n = MetaOp {
  metaOpIn     :: NodeSet n,
  metaOpOut    :: NodeSet n,
  metaOpInterm :: NodeSet n,
  metaOpPlan   :: forall m' . Monad m' => PlanT t n m' [Transition t n]
  }
showMetaOp :: MetaOp t a -> String
showMetaOp MetaOp {..} =
  printf
    "MetaOp %s -%s-> %s"
    (show $ toNodeList metaOpIn)
    (show $ toNodeList metaOpInterm)
    (show $ toNodeList metaOpOut)

instance AShow (MetaOp t n) where
  ashow' MetaOp{..} = recSexp "MetaOp" [
    ("metaOpOut", ashow' $ toNodeList metaOpOut),
    ("metaOpInterm", ashow' $ toNodeList metaOpInterm),
    ("metaOpIn", ashow' $ toNodeList metaOpIn)]

instance Semigroup (MetaOp t n) where
  op <> op' = MetaOp {
    metaOpIn=metaOpIn op <> metaOpIn op',
    metaOpOut=metaOpOut op <> metaOpOut op',
    metaOpInterm=metaOpInterm op <> metaOpInterm op',
    metaOpPlan=(++) <$> metaOpPlan op <*> metaOpPlan op'
    }

instance Monoid (MetaOp t n) where
  mempty = MetaOp {
    metaOpIn=mempty,
    metaOpOut=mempty,
    metaOpInterm=mempty,
    metaOpPlan=return []
    }
mopTriple :: MetaOp t n -> (NodeSet n, NodeSet n, NodeSet n)
mopTriple MetaOp{..} = (metaOpIn,metaOpOut,metaOpInterm)
{-# INLINE mopTriple #-}
instance Hashable (MetaOp t n) where
  hash = hash . mopTriple
  {-# INLINE hash #-}
  hashWithSalt s = hashWithSalt s . mopTriple
  {-# INLINE hashWithSalt #-}
instance Eq (MetaOp t n) where
  a == b = mopTriple a == mopTriple b
  {-# INLINE (==) #-}


-- Node procs
-- | NodeProc t n ()
type NodeProc t n w = NodeProc0 t n w w

type Scaling = Double
-- | A node process. The outer
type NodeProc0 t n w w0 =
  ArrProc w0 (PlanT t n Identity)

lowerNodeProc
  :: ZCoEpoch w0 ~ ZCoEpoch w
  => Conv w0 w
  -> NodeProc0 t n w w0
  -> NodeProc t n w
lowerNodeProc = convArrProc
{-# INLINE lowerNodeProc #-}
liftNodeProc
  :: ZCoEpoch w0 ~ ZCoEpoch w
  => Conv w w0
  -> NodeProc t n w
  -> NodeProc0 t n w w0
liftNodeProc = convArrProc
{-# INLINE liftNodeProc #-}

data PlanParams tag n
newtype Predicates n = Predicates { pNonComputables :: NodeSet n }
  deriving (Eq,Show,Generic)
instance AShow (Predicates n)
isEmptyPredicates :: Predicates n -> Bool
isEmptyPredicates = nsNull . pNonComputables
instance Semigroup (Predicates n) where
  p <> p' =
    Predicates { pNonComputables = pNonComputables p <> pNonComputables p' }
instance Monoid (Predicates n) where
  mempty = Predicates mempty
type CoPredicates n = NodeSet n
data PlanEpoch n =
  PlanEpoch { peCoPred :: CoPredicates n,peParams :: RefMap n Bool }
  deriving Generic
data PlanCoEpoch n =
  PlanCoEpoch { pcePred :: Predicates n,pceParams :: RefMap n Bool }
  deriving (Show,Eq,Generic)
instance  AShow (PlanCoEpoch n)
instance  AShow (PlanEpoch n)
instance Default (PlanEpoch n)
instance Monoid (PlanCoEpoch n) where
  mempty = PlanCoEpoch mempty mempty
instance Semigroup  (PlanCoEpoch n) where
  PlanCoEpoch a b <> PlanCoEpoch a' b' = PlanCoEpoch (a <> a') (b <> b')

-- | Tags to disambiguate the properties of historical and cost processes.
data HistTag
data CostTag

type CostParams tag n = SumTag (PlanParams tag n) (PlanMechVal tag n)
type NoMatProc tag t n =
  NodeRef n
  -> NodeProc t n (CostParams tag n) -- The mat proc constructed by the node under consideration
  -> NodeProc t n (CostParams tag n)

class ExtParams (PlanParams tag n) => PlanMech tag n where
  type PlanMechVal tag n :: *

  mcMechMapLens :: GCState t n :>: RefMap n (NodeProc t n (CostParams tag n))
  mcMkCost :: Proxy tag -> NodeRef n -> Cost -> PlanMechVal tag n
  mcIsMatProc :: NoMatProc tag t n
  mcCompStack :: NodeRef n -> BndR (CostParams tag n)


instance ExtParams (PlanParams CostTag n) where
  type ExtError (PlanParams CostTag n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams CostTag n) = PlanEpoch n
  type ExtCoEpoch (PlanParams CostTag n) = PlanCoEpoch n
  type ExtCap (PlanParams CostTag n) =
    Min (PlanMechVal CostTag n)
  extCombEpochs _ = planCombEpochs

instance ExtParams (PlanParams HistTag n) where
  type ExtError (PlanParams HistTag n) =
    IndexErr (NodeRef n)
  type ExtEpoch (PlanParams HistTag n) = PlanEpoch n
  type ExtCoEpoch (PlanParams HistTag n) = PlanCoEpoch n
  type ExtCap (PlanParams HistTag n) =
    Min (PlanMechVal HistTag n)
  extCombEpochs _ = planCombEpochs

type IsPlanParams tag n =
  (ExtError (PlanParams tag n) ~ IndexErr
     (NodeRef n)
  ,ExtEpoch (PlanParams tag n) ~ PlanEpoch n
  ,ExtCoEpoch (PlanParams tag n) ~ PlanCoEpoch n
  ,AShow (PlanMechVal tag n)
  ,Ord (PlanMechVal tag n)
  ,Semigroup (PlanMechVal tag n)
  ,Subtr (PlanMechVal tag n)
  ,Zero (PlanMechVal tag n)
  ,Zero (ExtCap (PlanParams tag n))
  ,AShow (ExtCap (PlanParams tag n))
  ,HasLens (ExtCap (PlanParams tag n)) (Min (PlanMechVal tag n))
  ,PlanMech tag n)

-- | When the coepoch is older than the epoch we must reset and get
-- a fresh value for the process. Otherwise the progress made so far
-- towards a value is valid and we should continue from there.
--
-- XXX: The cap may have changed though
planCombEpochs :: PlanCoEpoch n -> PlanEpoch n -> a -> MayReset a
planCombEpochs coepoch epoch a =
  if paramsMatch && predicatesMatch then DontReset a else ShouldReset
  where
    predicatesMatch =
      nsNull (peCoPred epoch)
      || nsDisjoint (pNonComputables $ pcePred coepoch) (peCoPred epoch)
    paramsMatch =
      and
      $ refIntersectionWithKey
        (const (==))
        (pceParams coepoch)
        (peParams epoch)
