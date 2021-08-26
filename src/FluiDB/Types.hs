{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE FunctionalDependencies    #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

module FluiDB.Types
  (GlobalError(..)
  ,IsGlobalError(..)
  ,errLift
  ,joinErr
  ,PreGlobalConf(..)
  ,GlobalConf(..)
  ,GlobalSolveROT
  ,GlobalSolveT
  ,hoistGlobalSolveT
  ,globalPopUniqueNum
  ,IsGlobalConf(..)
  ,stateLayer
  ,errLayer
  ,flipSE
  ,joinExcept
  ,SqlTypeVars
  ,GlobalUnMonad
  ,s2r
  ,r2s
  ,NodeReport(..)
  ,mkGCParams
  ,RunningConf(..)) where

import           Control.Monad.Cont
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Morph
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build.Monads.Class
import           Data.Codegen.SchemaAssocClass
import qualified Data.List.NonEmpty              as NEL
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.Optimizations
import           Data.Query.QuerySize
import           Data.Query.SQL.FileSet
import           Data.Query.SQL.Types
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.MTL
import           FluiDB.SumTypes                 as ST
import           GHC.Generics


data GlobalError e s t n =
  GlobalPlanningError (PlanningError t n)
  | GlobalPlanSanityError (PlanSanityError t n)
  | GlobalCodeBuildError (CodeBuildErr e s t n)
  | GlobalOptimizationError (OptimizationError e s t n)
  | GlobalTightenError (TightenErr e s)
  -- | GlobalSanityError (SanityError e s)
  | GlobalQNFError (QNFError e s)
  | GlobalClusterError (ClusterError e s)
  | CommandError String [String] Int
  | FileMissing FilePath
  | InsufficientBudget (Either (Int, Maybe Int) Int)
  | UnsolvableQuery Int
  -- There were no materialized tables to solve for.
  | NoGroundTruth
  | GlobalErrorMsg (AShowStr e s)
  deriving (Eq, Generic)
instance IsString (GlobalError e s t n) where
  fromString = GlobalErrorMsg . fromString
instance (AShowV e, AShowV s) => AShow (GlobalError e s t n)
instance AShowError e s (GlobalError e s t n)
class IsGlobalError e s t n a where
  toGlobalError :: a -> GlobalError e s t n
instance IsGlobalError e s t n String where
  toGlobalError = GlobalErrorMsg . mkAStr
instance IsGlobalError e s t n (AShowStr e s) where
  toGlobalError = GlobalErrorMsg
instance IsGlobalError e s t n (TightenErr e s) where
  toGlobalError = GlobalTightenError
instance IsGlobalError e s t n (OptimizationError e s t n) where
  toGlobalError = GlobalOptimizationError
instance IsGlobalError e s t n (PlanningError t n) where
  toGlobalError = GlobalPlanningError
instance IsGlobalError e s t n (PlanSanityError t n) where
  toGlobalError = GlobalPlanSanityError
instance IsGlobalError e s t n (CodeBuildErr e s t n) where
  toGlobalError = GlobalCodeBuildError
instance IsGlobalError e s t n (QNFError e s) where
  toGlobalError = GlobalQNFError
instance IsGlobalError e s t n (ClusterError e s) where
  toGlobalError = GlobalClusterError
instance (IsGlobalError e s t n a, IsGlobalError e s t n b) =>
         IsGlobalError e s t n (a ST.:+: b) where
  toGlobalError = either toGlobalError $ toGlobalError . unCType
instance IsGlobalError e s t n a => IsGlobalError e s t n (ST.Fin a) where
  toGlobalError = either toGlobalError undefined

errLift :: forall e s t n m a x . (Functor m, IsGlobalError e s t n a) =>
          ExceptT a m x -> ExceptT (GlobalError e s t n) m x
errLift = ExceptT . fmap (first toGlobalError) . runExceptT
joinErr :: Functor m => ExceptT a (ExceptT a m) x -> ExceptT a m x
joinErr = ExceptT . fmap join . runExceptT . runExceptT

data RunningConf = RunningConf {
  runningConfBudgetSearch :: Bool,
  runningUniqueNumber     :: Int
  } deriving (Generic, Show, Eq)
instance Default RunningConf
instance AShow RunningConf
globalPopUniqueNum :: Monad m => GlobalSolveT e s t n m Int
globalPopUniqueNum = do
  n <- gets (runningUniqueNumber . globalRunning)
  modify $ \gc -> gc{
    globalRunning=(globalRunning gc){
        runningUniqueNumber=1 + runningUniqueNumber (globalRunning gc)}}
  return n

data PreGlobalConf e0 e s =
  PreGlobalConf
  { pgcExpIso         :: (e -> ExpTypeSym' e0,ExpTypeSym' e0 -> e)
   ,pgcToUniq         :: Int -> e -> Maybe e
   ,pgcToFileSet      :: s -> Maybe FileSet -- Embedding of tables in filesets
   ,pgcPrimKeyAssoc   :: [(s,[e])]          -- Primary keys of each table
   ,pgcSchemaAssoc    :: SchemaAssoc e s     -- The schema of each table
   ,pgcTableSizeAssoc :: [(s,TableSize)]          -- Size of each table in bytes
  }

data GlobalConf e s t n = forall e0 . GlobalConf {
  globalTableSizeAssoc :: [(s,TableSize)],
  globalRunning        :: RunningConf,
  globalSchemaAssoc    :: SchemaAssoc e s,
  globalQueryCppConf   :: QueryCppConf e s,
  globalClusterConfig  :: ClusterConfig e s t n,
  globalGCConfig       :: GCConfig t n,
  globalMatNodes       :: [NodeRef n],
  globalExpTypeSymIso  :: (e -> ExpTypeSym' e0,ExpTypeSym' e0 -> e)
  }

-- Warning: we are not taking int accont scores.
data NodeReport e s t n = NodeReport {
  nrRef      :: NodeRef n,
  nrMatState :: NodeState,
  nrSize     :: ([TableSize],Double),
  nrIsInterm :: Bool,

  nrQuery    :: [Query e s]
  } deriving (Read, Show, Generic)
instance ARead (Query e s) => ARead (NodeReport e s t n)
instance AShow (Query e s) => AShow (NodeReport e s t n)

mkGCParams :: forall e s t n .
             [NodeReport e s t n]
           -> (GCConfig t n, GCState t n)
mkGCParams = foldl go (def,def)
  where
    go :: (GCConfig t n,GCState t n)
       -> NodeReport e s t n
       -> (GCConfig t n,GCState t n)
    go (gcc@GCConfig {..},gcs@GCState {..}) NodeReport {..} =
      (gcc { nodeSizes = refInsert nrRef nrSize nodeSizes
            ,intermediates = (if nrIsInterm then nsInsert nrRef else id)
               intermediates
           }
      ,gcs { epochs = epochs
               `onHead` (\ep -> ep { nodeStates = refInsert nrRef nrMatState
                                       $ nodeStates
                                       $ NEL.head epochs
                                   })
           })
    onHead :: NEL.NonEmpty a -> (a -> a) -> NEL.NonEmpty a
    onHead (a NEL.:| as) f = f a NEL.:| as

type GlobalSolveT e s t n m =
  ExceptT (GlobalError e s t n) (StateT (GlobalConf e s t n) m)
type GlobalSolveROT e s t n m =
  ExceptT (GlobalError e s t n) (ReaderT (GlobalConf e s t n) m)

s2r :: Monad m => ExceptT e (StateT s m) r -> ExceptT e (ReaderT s m) (s, r)
s2r (ExceptT (StateT f)) = ExceptT $ ReaderT $ \s -> do
  (ret, newState) <- f s
  return $ (newState,) <$> ret
r2s :: Monad m => ExceptT e (ReaderT s m) a -> ExceptT e (StateT s m) a
r2s = hoist $ \(ReaderT f) -> StateT $ \s -> (,s) <$> f s
type GlobalUnMonad e s t n a = (Either (GlobalError e s t n) a, GlobalConf e s t n)
hoistGlobalSolveT :: Monad m =>
                    (m (GlobalUnMonad e s t n a) -> g (GlobalUnMonad e s t n a))
                  -> GlobalSolveT e s t n m a
                  -> GlobalSolveT e s t n g a
hoistGlobalSolveT f stM =
  ExceptT $ StateT $ \conf -> f ((runStateT $ runExceptT stM) conf)

class IsGlobalConf e s t n a where
  modGlobalConf :: a -> GlobalConf e s t n -> GlobalConf e s t n
  getGlobalConf :: GlobalConf e s t n -> a
instance IsGlobalConf e s t n (GCConfig t n) where
  getGlobalConf = globalGCConfig
  modGlobalConf x qnf = qnf{globalGCConfig=x}
instance IsGlobalConf e s t n (GCState t n) where
  getGlobalConf GlobalConf{..} =
    setNodeStatesToGCState globalMatNodes globalGCConfig def
  modGlobalConf st qnf@GlobalConf{..} =
    qnf{
    globalMatNodes=nodesInSt
      [Initial Mat, Concrete NoMat Mat, Concrete Mat Mat]
    } where
    graph = propNet globalGCConfig
    nodesInSt = runIdentity . dropReader (Identity st) . nodesInState' graph

instance IsGlobalConf e s t n (GBState t n) where
  getGlobalConf GlobalConf {..} = def { gbPropNet = propNet globalGCConfig }
  modGlobalConf x qnf =
    qnf { globalGCConfig = (globalGCConfig qnf) { propNet = gbPropNet x } }
instance IsGlobalConf e s t n (QueryCppConf e s) where
  getGlobalConf = globalQueryCppConf
  modGlobalConf x qnf = qnf{globalQueryCppConf=x}
instance IsGlobalConf e s t n (CBState e s t n) where
  getGlobalConf = emptyCBState . getGlobalConf
  modGlobalConf x qnf = qnf{
    globalQueryCppConf=(cbQueryCppConf x){
        defaultQueryFileCache=cbQueryFileCache x
        }
    }
instance IsGlobalConf e s t n (ClusterConfig e s t n) where
  getGlobalConf = globalClusterConfig
  modGlobalConf x qnf = qnf{globalClusterConfig=x}

stateLayer :: forall m e s t n a x . (Monad m, IsGlobalConf e s t n a) =>
             a
           -> GlobalSolveT e s t n (StateT a m) x
           -> GlobalSolveT e s t n m x
stateLayer st val = ExceptT $ StateT $ \conf -> do
  ((eitherRet, newConf), newSt) <- (`runStateT` st) $ runStateT (runExceptT val) conf
  return (eitherRet, modGlobalConf newSt newConf)

errLayer :: forall a e s t n m x .
           (Monad m, IsGlobalError e s t n a) =>
           GlobalSolveT e s t n (ExceptT a m) x
         -> GlobalSolveT e s t n m x
errLayer = joinErr . hoist (errLift . flipSE)

flipSE :: forall e s a m . Monad m => StateT s (ExceptT e m) a -> ExceptT e (StateT s m) a
-- f              :: s -> ExceptT e m (a, s)
-- runExceptT . f :: s -> m (Either e (a, s))
flipSE (StateT f) = ExceptT $ StateT $ \s -> do
  eitherRet <- runExceptT $ f s
  case eitherRet of
    Left e        -> return (Left e, s)
    Right (x, s') -> return (Right x, s')

joinExcept :: Monad m =>
             ExceptT e (ExceptT e m) x
           -> ExceptT e m x
joinExcept (ExceptT (ExceptT mx)) = ExceptT $ join <$> mx
type SqlTypeVars e s t n =
  (e ~ ExpTypeSym,
   s ~ Maybe FileSet,
   t ~ (),
   n ~ ())
