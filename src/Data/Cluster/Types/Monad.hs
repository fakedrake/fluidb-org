{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}

module Data.Cluster.Types.Monad
  (ClusterError(..)
  ,ClusterConfig(..)
  ,ClustPropagators(..)
  ,ClustBuildCache(..)
  ,InsPlanRes(..)
  ,QueryForest(..)
  ,CGraphBuilderT
  ,CPropagator
  ,ACPropagator
  ,PlanCluster
  ,PropCluster
  ,Defaulting
  ,CPropagatorPlan
  ,Tunnel(..)
  ,tqueryToForest
  ,forestToQuery
  ,queryToForest
  ,freeToForest
  ,regCall
  ,showRegCall
  ,clearClustBuildCache
  ,ifDefaultingEmpty
  ,demoteDefaulting
  ,hoistCGraphBuilderT
  ,promoteDefaulting
  ,getDefaultingFull
  ,getDefaultingDef
  ,defaultingLe
  ,getDef) where

import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Morph
import           Control.Monad.State
import           Control.Utils.Free
import           Data.Bipartite
import           Data.Bits
import           Data.Cluster.Types.Clusters
import qualified Data.HashMap.Strict           as HM
import qualified Data.HashSet                  as HS
import qualified Data.IntMap                   as IM
import           Data.List.Extra
import qualified Data.List.NonEmpty            as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.Optimizations.Echo
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.EmptyF
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           GHC.Generics

data ClusterError e s =
  ClusterQNFError (QNFError e s)
  | ClusterAMsg (AShowStr e s)
  | InsertingBottom s [s]
  deriving (Eq, Generic)
instance AShowError e s (ClusterError e s)
instance (AShowV e, AShowV s) => AShow (ClusterError e s)

data Tunnel n = Tunnel { tExits :: [NodeRef n],tEntrys :: [NodeRef n] }

-- | ClusterConfig is empty in the initial stage.
data ClusterConfig e s t n =
  ClusterConfig
  { qnfTunels :: IM.IntMap (Tunnel n)
    -- |If this is False throw an en error when there is a bottom that
    -- does not correspond to a node.
   ,qnfInsertBottoms :: Bool
    -- | All nodes of each cluster and their corresponding cluister
   ,qnfToClustMap :: HM.HashMap (QNFQuery e s) [AnyCluster e s t n]
   ,nrefToQnfs :: RefMap n [QNFQuery e s]
    -- ^ All these will be equiv: (schema, query)
   ,trefToQnf :: RefMap t (QNFQuery e s)
   ,qnfTableColumns :: s -> Maybe [e]
   ,qnfNodePlans :: RefMap n (Defaulting (QueryPlan e s))
    -- ^ The plan that we expect to find in each node.
   ,qnfPropagators
      :: HM.HashMap (AnyCluster e s t n) (ClustPropagators e s t n)
    -- Update the map between nodes and queries. The same cluster is
    -- associated with multiple equivalent name translations with
    -- equivalent qnfs. Each propagator in the list deals withy a
    -- separate naming pattern.
   ,clustBuildCache :: ClustBuildCache e s t n
  }
newtype ClustPropagators e s t n = ClustPropagators {
  planPropagators :: [ACPropagatorAssoc e s t n]
  } deriving Generic
instance Default (ClustPropagators e s t n)
data InsPlanRes e s t n = InsPlanRes {
  insPlanRef   :: NodeRef n,
  insPlanNQNFs :: HS.HashSet (NQNFQueryI e s),
  insPlanQuery :: Query e (s,QueryPlan e s)
  } deriving Generic
instance (AShowV e,AShowV s) => AShow (InsPlanRes e s t n)
data QueryForest e s =
  QueryForest
  { qfHash :: Int
   ,qfQueries
      :: Either (NEL.NonEmpty (TQuery e (QueryForest e s))) (s,QueryPlan e s)
  }
  deriving Generic

-- XXX: Each tree is equivalent to each other  so calculating the
-- qnfquery we can get the hash for "free".
freeToForest
  :: Hashables2 e s
  => Free (Compose NEL.NonEmpty (TQuery e)) (s,QueryPlan e s)
  -> QueryForest e s
freeToForest = \case
  FreeT (Identity (Free (Compose a))) -> let v = fmap2 freeToForest a
    in QueryForest {qfHash=hash v,qfQueries=Left v}
  FreeT (Identity (Pure a)) -> QueryForest {qfHash=hash a,qfQueries=Right a}
forestToQuery :: Hashables2 e s => QueryForest e s -> Query e (s,QueryPlan e s)
forestToQuery =
  either
    (forestToQuery <=< either (fmap tqueryToForest) id . tqQuery . NEL.head)
    Q0
  . qfQueries

queryToForest :: Hashables2 e s => Query e (QueryForest e s) -> QueryForest e s
queryToForest = tqueryToForest . tunnelQuery

tqueryToForest :: Hashables2 e s => TQuery e (QueryForest e s) -> QueryForest e s
tqueryToForest q =
  let v = return q in QueryForest { qfHash = hash v,qfQueries = Left v }

instance Eq (QueryForest e s) where a == b = qfHash a == qfHash b
instance Hashable (QueryForest e s) where
  hashWithSalt s x = (s * 16777619) `xor` qfHash x
  hash = qfHash
data ClustBuildCache e s t n = ClustBuildCache {
  qnfBuildCache :: QNFCache e s,
  queriesCache  :: HM.HashMap (QueryForest e s) (InsPlanRes e s t n),
  callCount     :: HM.HashMap String Int
  } deriving Generic
instance Default (ClustBuildCache e s t n)
clearClustBuildCache :: MonadState (ClusterConfig e s t n) m => m ()
clearClustBuildCache = modify $ \cc -> cc{clustBuildCache=def}

-- | Count function calls
showRegCall :: MonadState (ClusterConfig e s t n) m => m ()
showRegCall = do
  ls <- gets $ sortOn snd . HM.toList . callCount . clustBuildCache
  traceM "Functions called:"
  case ls of
    [] -> traceM "<No functions called to show>"
    _  -> forM_ ls $ \(n,c) -> traceM $ printf "\t%s -> %d" n c

regCall :: MonadState (ClusterConfig e s t n) m => String -> m ()
regCall call = modify $ \cc -> cc{
  clustBuildCache=(clustBuildCache cc){
      callCount=HM.alter (Just . (+1) . fromMaybe 0) call
        $ callCount
        $ clustBuildCache cc}}

-- | (Propagator,In :-> Out sym)
type ACPropagatorAssoc e s t n =
  (ACPropagator (QueryPlan e s) e s t n,[(PlanSym e s,PlanSym e s)])
-- | A value that either has a default value or defaults.
data Defaulting a = DefaultingEmpty
                  | DefaultingDef a
                  | DefaultingFull a a
                  -- ^ DefaultingFull default_value actual_value
  deriving (Eq, Generic, Show, Read, Functor, Traversable, Foldable)
instance Hashable a => Hashable (Defaulting a)
instance Applicative Defaulting where
  pure = DefaultingDef
  -- | We want f <$> a <*> b <*> c <*> d to be the minimum of a b c d.
  -- also we want:
  --
  --   (++) <$> DefaultingFull x x' <*> DefaultingFull y y'
  --     ==> DefaultingFull (x ++ y) (x' ++ y)
  DefaultingEmpty <*> _                       = DefaultingEmpty
  _ <*> DefaultingEmpty                       = DefaultingEmpty
  DefaultingDef f <*> DefaultingDef x         = DefaultingDef $ f x
  DefaultingFull df vf <*> DefaultingFull d v = DefaultingFull (df d) (vf v)
  DefaultingFull _ vf <*> DefaultingDef v     = DefaultingDef (vf v)
  DefaultingDef f <*> DefaultingFull _ v      = DefaultingDef (f v)
-- | Alternative is left biased, ie l <|> r, l sets the default
-- value when available.
instance Alternative Defaulting where
  empty = DefaultingEmpty
  a <|> DefaultingEmpty                    = a
  DefaultingEmpty <|> a                    = a
  a <|> (DefaultingDef _)                  = a
  (DefaultingDef d) <|> DefaultingFull _ a = DefaultingFull d a
  a <|> _                                  = a

promoteDefaulting :: Defaulting a -> Defaulting a
promoteDefaulting = \case
  DefaultingEmpty        -> DefaultingEmpty
  DefaultingDef x        -> DefaultingFull x x
  d@(DefaultingFull _ _) -> d
demoteDefaulting :: Defaulting a -> Defaulting a
demoteDefaulting = \case
  DefaultingFull a _ -> DefaultingDef a
  x                  -> x
defaultingLe :: Defaulting e -> Defaulting e -> Bool
defaultingLe x y = lv x <= lv y where
  lv :: Defaulting x -> Int
  lv = \case
    DefaultingEmpty    -> 0
    DefaultingDef _    -> 1
    DefaultingFull _ _ -> 2

getDef :: Defaulting a -> Maybe a
getDef d = getDefaultingFull d <|> getDefaultingDef d
getDefaultingDef :: Defaulting a -> Maybe a
getDefaultingDef = \case
  DefaultingDef a    -> Just a
  DefaultingFull d _ -> Just d
  _                  -> Nothing
getDefaultingFull :: Defaulting a -> Maybe a
getDefaultingFull = \case
  DefaultingFull _ v -> Just v
  _                  -> Nothing
ifDefaultingEmpty :: Defaulting a -> x -> x -> x
ifDefaultingEmpty p isEmpty isntEmpty = case p of
  DefaultingEmpty -> isEmpty
  _               -> isntEmpty

instance AShow a => AShow (Defaulting a)
type PropCluster a f e s t n =
  AnyCluster' (PlanSym e s) (WMetaD (Defaulting a) f) t n
type PlanCluster f e s t n = PropCluster (QueryPlan e s) f e s t n
type EndoE e s x = x -> Either (AShowStr e s) x
type ACPropagator a e s t n = EndoE e s (PropCluster a NodeRef e s t n)
type CPropagator a c e s t n =
  EndoE e s (c EmptyF (WMetaD (Defaulting a) NodeRef) t n)
type CPropagatorPlan c e s t n = CPropagator (QueryPlan e s) c e s t n

instance Hashables2 e s => Default (ClusterConfig e s t n) where
  def =
    ClusterConfig
    { qnfTunels = mempty
     ,qnfInsertBottoms = False
     ,qnfToClustMap = mempty
     ,nrefToQnfs = mempty
     ,trefToQnf = mempty
     ,qnfTableColumns = const Nothing
     ,qnfPropagators = mempty
     ,qnfNodePlans = mempty
     ,clustBuildCache = def
    }

type CGraphBuilderT e s t n m =
  ExceptT
    (ClusterError e s)
    (StateT (ClusterConfig e s t n) (GraphBuilderT t n m))
hoistCGraphBuilderT :: Monad m =>
                      (forall x . m x -> g x)
                    -> CGraphBuilderT e s t n m a
                    -> CGraphBuilderT e s t n g a
hoistCGraphBuilderT f = hoist (hoist (hoistGraphBuilderT f))
