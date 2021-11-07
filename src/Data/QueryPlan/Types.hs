{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DefaultSignatures     #-}
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
{-# LANGUAGE UndecidableInstances  #-}

module Data.QueryPlan.Types
  (Transition(..)
  ,HistTag
  ,CostTag
  ,HistProc
  ,CostProc
  ,PCost
  ,QueryHistory(..)
  ,PlanT
  ,IsMatable
  ,PlanSearchScore(..)
  ,MetaOp(..)
  ,pushHistory
  ,showMetaOp
  ,throwPlan
  ,GCState(..)
  ,GCConfig(..)
  ,GCEpoch(..)
  ,NodeState(..)
  ,PlanningError(..)
  ,IsMat(..)
  ,BotMonad(..)
  ,PlanSanityError(..)
  ,ProvenanceAtom(..)
  ,MonadLogic
  ,hoistPlanT
  ,costAsInt
  ,Count
  ,NodeProc
  ,PlanParams
  ,SumTag
  ,bot
  ,top
  ,runPlanT'
  ,liftPlanT
  ,trM
  ,mplusPlanT) where


import           Control.Antisthenis.Bool
import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Trans.Maybe
import           Control.Monad.Writer            hiding (Sum)
import           Data.Bipartite
import           Data.Function
import           Data.Group
import qualified Data.HashSet                    as HS
import           Data.IntMap                     (Key)
import           Data.List
import qualified Data.List.NonEmpty              as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Query.QuerySize
import           Data.QueryPlan.AntisthenisTypes
import           Data.QueryPlan.Comp
import           Data.QueryPlan.CostTypes
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.HCntT
-- import           Data.Utils.HContT
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.MinElem
import           GHC.Generics                    (Generic)
import           GHC.Stack
import           Prelude                         hiding (filter, lookup)

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
-- only by the *first* occuring trigger. Note that the outputs are curated
data Transition t n
  = Trigger [NodeRef n] (NodeRef t) [NodeRef n]
  | RTrigger [NodeRef n] (NodeRef t) [NodeRef n] -- read t write
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

#ifdef EXAMPLE
provenanceAsBool :: ProvenanceAtom -> Bool
provenanceAsBool = \case
  EitherLeft  -> False
  SplitLeft   -> False
  EitherRight -> True
  SplitRight  -> True
#endif

type PCost = Comp Cost
type IsMatable = Bool
type NodeProc t n w = ArrProc w (PlanT t n Identity)
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
#ifdef GHCI
trM msg = modify $ \gss -> gss{gcLog=msg:gcLog gss}
getGcLog :: Monad m => PlanT t n m [String]
getGcLog = reverse . gcLog <$> get
#else
#ifdef VERBOSE_SOLVING
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

trM = traceM <=< traceMsg
#else
trM = const $ return ()
#endif
{-# INLINE trM #-}
#endif

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
-- instance (Partitionable m, Ord v,Group v,BotMonad m) =>
--          BotMonad (HContT v r m) where
--   eitherl' = eitherlHCont
instance (IsHeap h,Monad m) => BotMonad (HCntT h r m) where
  eitherl' = (<//>)

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

#ifdef EXAMPLE
-- |Incrment the tree depth counter.
incTreeProp
  :: MonadPlus m
  => String
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
#endif

data PlanSanityError t n
  = MissingScore (NodeRef n) (RefMap n Double)
  | MissingSize (NodeRef n) (RefMap n [TableSize])
  | NoError
  deriving (Eq, Show, Generic)
instance AShow (PlanSanityError t n)
data PlanSearchScore = PlanSearchScore {
  psFrontierCost :: Double, -- The cost of the current
  psOtherCost    :: Maybe Double
  } deriving (Generic,Show,Eq)
instance MinElem PlanSearchScore where
  minElem =
    PlanSearchScore { psOtherCost = Nothing,psFrontierCost = 0 }
instance Ord PlanSearchScore where
  compare l r = toI l `compareM` toI r
    where
      compareM Nothing Nothing   = EQ
      compareM Nothing _         = GT
      compareM _ Nothing         = LT
      compareM (Just x) (Just y) = compare x y
      toI PlanSearchScore {..} = (psFrontierCost +) <$> psOtherCost
  {-# INLINE compare #-}
instance Monoid PlanSearchScore where
  {-# INLINE mempty #-}
  mempty = PlanSearchScore  {
    psFrontierCost=0,
    psOtherCost=Nothing
    }
instance Group PlanSearchScore where
  invert l = l{psFrontierCost = -(psFrontierCost l)}
instance Semigroup PlanSearchScore where
  {-# INLINE (<>) #-}
  l <> r = PlanSearchScore{
    psFrontierCost=psFrontierCost l + psFrontierCost r,
    psOtherCost=psOtherCost r <|> psOtherCost l
    }
type MonadLogic m =
  (HaltKey m ~ PlanSearchScore,MonadPlus m,BotMonad m,MonadHalt m)
-- type MonadHaltD m = (HValue m ~ PlanSearchScore, MonadHalt m)
type PlanT t n m =
  StateT (GCState t n) (ReaderT (GCConfig t n) (ExceptT (PlanningError t n) m))

hoistPlanT
  :: (m (Either (PlanningError t n) (a,GCState t n))
      -> g (Either (PlanningError t n) (a,GCState t n)))
  -> PlanT t n m a
  -> PlanT t n g a
hoistPlanT = hoistStateT . hoistReaderT . hoistExceptT
  where
    hoistStateT :: (m (a,s) -> g (a,s)) -> StateT s m a -> StateT s g a
    hoistStateT f (StateT m) = StateT $ \s -> f $ m s
    hoistReaderT :: (m a -> g a) -> ReaderT s m a -> ReaderT s g a
    hoistReaderT f (ReaderT m) = ReaderT $ \r -> f $ m r
    hoistExceptT
      :: (m (Either e a) -> g (Either e a)) -> ExceptT e m a -> ExceptT e g a
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
mplusPlanT' :: MonadPlus m =>
              StateT s (ReaderT r (ExceptT e m)) a
            -> StateT s (ReaderT r (ExceptT e m)) a
            -> StateT s (ReaderT r (ExceptT e m)) a
mplusPlanT' s s' = s `mplusState` s'
  where
    mplusState (StateT f) (StateT f') =
      StateT $ \s0 -> f s0 `mplusReader` f' s0
      where
        mplusReader (ReaderT v0) (ReaderT v0') =
          ReaderT $ \r -> v0 r `mplusExcept` v0' r
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
