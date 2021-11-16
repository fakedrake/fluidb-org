{-# LANGUAGE CPP                    #-}
{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE InstanceSigs           #-}
{-# LANGUAGE KindSignatures         #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE PolyKinds              #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE TypeSynonymInstances   #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-unused-foralls -Wno-name-shadowing -Wno-unused-top-binds #-}

module Data.Codegen.Build.Monads.Class
  (MonadReadScope(..)
  ,MonadCodeBuilder(..)
  ,MonadCodeError
  ,QueryCppConf(..)
  ,MonadSoftCodeBuilder(..)
  ,MonadCheckpoint(..)
  ,MonadCodeCheckpoint
  ,CodeBuildErr(..)
  ,CBState(..)
  ,MonadWriteScope(..)
  ,ScopeEnv(..)
  ,QueryShape
  ,MonadSchemaScope
  ,throwCodeErrStr
  ,emptyCBState
  ,evalAggrEnv
  ,evalQueryEnv
  ,getClassNames
  ,getQueries
  ,getScopeQueries
  ,getQueryCppConf
  ,putQueryCppConf
  ,runSoftCodeBuilder
  ,tellClassM
  ,tellFunctionM
  ,tellInclude
  ,throwCodeErr
  ,tryInsert) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Trans.Writer
import           Data.Cluster.Types
import           Data.Codegen.Build.Types
import qualified Data.CppAst                as CC
import           Data.Either
import           Data.Foldable
import qualified Data.HashMap.Strict        as HM
import qualified Data.HashSet               as HS
import           Data.Maybe
import           Data.Monoid
import           Data.Query.Algebra
import           Data.QueryPlan.Types
import           Data.String
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Prelude                    hiding (exp)

class MonadError (CodeBuildErr e s t n) m =>
      MonadCodeError e s t n m
throwCodeErr :: MonadCodeError e s t n m => CodeBuildErr e s t n -> m a
throwCodeErr = throwError
throwCodeErrStr :: MonadCodeError e s t n m => String -> m a
throwCodeErrStr = throwError . BuildErrMsg

emptyCBState :: QueryCppConf e s -> CBState e s t n
gemptyCBState conf@QueryCppConf {..} =
  CBState
  { cbMatNodePlans = mempty
   ,cbQueryCppConf = conf
   ,cbIncludes = mempty
   ,cbClasses = mempty
   ,cbFunctions = mempty
   ,cbQueryFileCache = defaultQueryFileCache
   ,cbDataDir = dataDir
  }

instance MonadCodeError e s t n (Either (CodeBuildErr e s t n))
instance MonadCodeError e s t n m
  => MonadCodeError e s t n (ListT m)
instance MonadCodeError e s t n m =>
         MonadCodeError e s t n (StateT (ScopeEnv c AggrFunction) m)
instance MonadCodeError e s t n m =>
         MonadCodeError e s t n (StateT Int m)
instance (Monoid a, MonadCodeError e s t n m) =>
         MonadCodeError e s t n (WriterT a m)
instance MonadCodeError e s t n m => MonadCodeError e s t n (ReaderT a m)
instance Monad m => MonadCodeError e s t n (ExceptT (CodeBuildErr e s t n) m)
instance MonadCodeError e s t n m =>
         MonadCodeError e s t n (StateT (CBState e s t n) m)

class Monad m => MonadCodeBuilder e s t n m | m -> e, m -> s, m -> t, m -> n where
  getCBState :: m (CBState e s t n)
  default getCBState ::
    (MonadTrans tr, Monad m', MonadCodeBuilder e s t n m', m ~ tr m') =>
    m (CBState e s t n)
  getCBState = lift getCBState
  {-# INLINE getCBState #-}
  putCBState :: CBState e s t n -> m ()
  default putCBState ::
    (MonadTrans tr, Monad m', MonadCodeBuilder e s t n m', m ~ tr m') =>
    CBState e s t n -> m ()
  putCBState = lift . putCBState
  {-# INLINE putCBState #-}
instance Monad m => MonadCodeBuilder e s t n (StateT (CBState e s t n) m) where
  getCBState = get
  putCBState = put
instance MonadCodeBuilder e s t n m =>
         MonadCodeBuilder e s t n (StateT (ScopeEnv c AggrFunction) m)
instance (Monoid a, MonadCodeBuilder e s t n m) =>
         MonadCodeBuilder e s t n (WriterT a m)
instance MonadCodeBuilder e s t n m => MonadCodeBuilder e s t n (StateT Int m)
instance MonadCodeBuilder e s t n m => MonadCodeBuilder e s t n (ReaderT a m)
instance MonadCodeBuilder e s t n m => MonadCodeBuilder e s t n (ListT m)
instance MonadCodeBuilder e s t n m => MonadCodeBuilder e s t n (ExceptT err m)

-- |Individual accessors for the members.
getQueryCppConf :: MonadCodeBuilder e s t n m => m (QueryCppConf e s)
getQueryCppConf = cbQueryCppConf <$> getCBState
putQueryCppConf :: MonadCodeBuilder e s t n m => QueryCppConf e s -> m ()
putQueryCppConf c = do {st <- getCBState; putCBState st{cbQueryCppConf=c}}

-- | Compute the class then register it if it's not already
-- registered. If it's registereed reverse the effects of computing
-- it. Return the class that ends up in the registry.
tellClassM :: (MonadCheckpoint m,  MonadCodeBuilder e s t n m,
              MonadCodeError e s t n m) =>
             m (CC.Class CC.CodeSymbol)
           -> m (CC.Class CC.CodeSymbol)
tellClassM cM = do
  st <- getCBState
  (classes, cls) <- tryInsert (cbClasses <$> getCBState) cM
  putCBState st{cbClasses=classes}
  return cls

getClassNames :: (MonadCheckpoint m,  MonadCodeBuilder e s t n m,
                 MonadCodeError e s t n m) =>
                m [CC.Symbol CC.CodeSymbol]
getClassNames = fmap CC.className . toList . cbClasses <$> getCBState

-- | Compute the function then register it if it's not already
-- registered. If it's registereed reverse the effects of computing
-- it. Return the function that ends up in the registry.
tellFunctionM :: (MonadCheckpoint m,  MonadCodeBuilder e s t n m,
                 MonadCodeError e s t n m) =>
                m (CC.Function CC.CodeSymbol)
              -> m (CC.Function CC.CodeSymbol)
tellFunctionM fM = do
  st <- getCBState
  (functions, func) <- tryInsert (cbFunctions <$> getCBState) fM
  putCBState st{cbFunctions=functions}
  return func

tellInclude :: MonadCodeBuilder e s t n m => CC.Include -> m ()
tellInclude c = do
  st <- getCBState
  putCBState st{cbIncludes=c `HS.insert` cbIncludes st}

-- | A monad that creates unames
class Monad m => MonadSoftCodeBuilder m where
  mkUSymbol :: String -> m (CC.Symbol CC.CodeSymbol)
  default mkUSymbol :: (MonadTrans t, Monad m', MonadSoftCodeBuilder m', m ~ t m') =>
                       String -> m (CC.Symbol CC.CodeSymbol)
  mkUSymbol = lift . mkUSymbol
  {-# INLINE mkUSymbol #-}

instance Monad m => MonadSoftCodeBuilder (StateT Int m) where
  mkUSymbol name = do
    i <- get
    put (i+1)
    return $ CC.Symbol $ CC.UniqueSymbolDef name i
  {-# INLINE mkUSymbol #-}
instance MonadSoftCodeBuilder m => MonadSoftCodeBuilder (ReaderT (ClusterConfig e s t n) m)
instance MonadSoftCodeBuilder m
  => MonadSoftCodeBuilder (ReaderT (ClusterConfig e s t n,GCState t n) m)
instance MonadSoftCodeBuilder m
  => MonadSoftCodeBuilder (StateT (ScopeEnv c a) m)
instance MonadSoftCodeBuilder m => MonadSoftCodeBuilder (ReaderT (ScopeEnv c a) m)

instance (Monoid a, MonadSoftCodeBuilder m) =>
         MonadSoftCodeBuilder (WriterT a m) where
  mkUSymbol = lift . mkUSymbol
instance MonadSoftCodeBuilder m => MonadSoftCodeBuilder (ListT m) where
  mkUSymbol = lift . mkUSymbol

-- Readable scope
type MonadSchemaScope c e s m =
  (Hashables2 e s,MonadReadScope c (QueryShape e s) m)
class (Traversable c,Foldable c,Monad m) => MonadReadScope c a m | m -> c where
  getScope :: m (ScopeEnv c a)
instance (Traversable c,Foldable c,Monad m)
  => MonadReadScope c (QueryShape e s) (ReaderT (ScopeEnv c (QueryShape e s)) m) where
  getScope = ask
  {-# INLINE getScope #-}
instance (Traversable c,Foldable c,Monad m)
  => MonadReadScope c AggrFunction (StateT (ScopeEnv c AggrFunction) m) where
  getScope = get
  {-# INLINE getScope #-}
instance (Monoid a,MonadReadScope c (QueryShape e s) m)
  => MonadReadScope c (QueryShape e s) (WriterT a m) where
  getScope = lift getScope
  {-# INLINE getScope #-}
instance MonadReadScope c (QueryShape e s) m
  => MonadReadScope c (QueryShape e s) (StateT (ScopeEnv c AggrFunction) m) where
  getScope = lift getScope
  {-# INLINE getScope #-}
instance MonadReadScope c AggrFunction m
  => MonadReadScope c AggrFunction (ReaderT (ScopeEnv c (QueryShape e s)) m) where
  getScope = lift getScope
  {-# INLINE getScope #-}
instance MonadReadScope c a m => MonadReadScope c a (ListT m) where
  getScope = lift getScope
  {-# INLINE getScope #-}


-- Writable scope
class Monad m => MonadWriteScope c a m where
  putScope :: ScopeEnv c a -> m ()
instance Monad m =>
         MonadWriteScope c AggrFunction (StateT (ScopeEnv c AggrFunction) m) where
  putScope = put
  {-# INLINE putScope #-}
instance MonadWriteScope c AggrFunction m =>
         MonadWriteScope c AggrFunction (ReaderT (ScopeEnv c (QueryShape e s)) m) where
  putScope = lift . putScope
  {-# INLINE putScope #-}
instance (Monoid a, MonadWriteScope c AggrFunction m) =>
         MonadWriteScope c AggrFunction (WriterT a m) where
  putScope = lift . putScope
  {-# INLINE putScope #-}

type family CheckpointTypeDefault m where
  CheckpointTypeDefault (StateT s m') = (s, CheckpointType m')
type family CheckpointTypeLift (m :: * -> *) where
  CheckpointTypeLift (t m') = CheckpointType m'

-- | With this we can run computations and then reverse the side
-- effects. This is useful when we want to check if
class Monad m => MonadCheckpoint m where
  type CheckpointType m :: *
  getCheckpoint :: m (CheckpointType m)
  restoreCheckpoint :: CheckpointType m -> m ()

instance MonadCheckpoint m => MonadCheckpoint (StateT (ScopeEnv c a) m) where
  type CheckpointType (StateT (ScopeEnv c a) m) =
    CheckpointTypeLift (StateT (ScopeEnv c a) m)
  getCheckpoint = lift getCheckpoint
  {-# INLINE getCheckpoint #-}
  restoreCheckpoint = lift . restoreCheckpoint
  {-# INLINE restoreCheckpoint #-}
instance MonadCheckpoint m => MonadCheckpoint (ListT m) where
  type CheckpointType (ListT m) = CheckpointTypeLift (ListT m)
  getCheckpoint = lift getCheckpoint
  {-# INLINE getCheckpoint #-}
  restoreCheckpoint = lift . restoreCheckpoint
  {-# INLINE restoreCheckpoint #-}
instance MonadCheckpoint m => MonadCheckpoint (ReaderT (ScopeEnv c a) m) where
  type CheckpointType (ReaderT (ScopeEnv c a) m) =
    CheckpointTypeLift (ReaderT (ScopeEnv c a) m)
  getCheckpoint = lift getCheckpoint
  {-# INLINE getCheckpoint #-}
  restoreCheckpoint = lift . restoreCheckpoint
  {-# INLINE restoreCheckpoint #-}
instance MonadCheckpoint m => MonadCheckpoint (ReaderT (ClusterConfig e s t n) m) where
  type CheckpointType (ReaderT (ClusterConfig e s t n) m) =
    CheckpointTypeLift (ReaderT (ClusterConfig e s t n) m)
  getCheckpoint = lift getCheckpoint
  {-# INLINE getCheckpoint #-}
  restoreCheckpoint = lift . restoreCheckpoint
  {-# INLINE restoreCheckpoint #-}
instance MonadCheckpoint m => MonadCheckpoint (ReaderT (ClusterConfig e s t n,GCState t n) m) where
  type CheckpointType (ReaderT (ClusterConfig e s t n,GCState t n) m) =
    CheckpointTypeLift (ReaderT (ClusterConfig e s t n,GCState t n) m)
  getCheckpoint = lift getCheckpoint
  {-# INLINE getCheckpoint #-}
  restoreCheckpoint = lift . restoreCheckpoint
  {-# INLINE restoreCheckpoint #-}

tryInsert :: (Foldable t, Traversable t,
             Eq (t (Either CC.CodeSymbol CC.CodeSymbol)),
             Hashable (t (Either CC.CodeSymbol CC.CodeSymbol)),
             MonadCheckpoint m, MonadCodeError e s t' n m) =>
             m (CC.CodeCache t)
           -> m (t CC.CodeSymbol)
           -> m (CC.CodeCache t, t CC.CodeSymbol)
tryInsert ccM tcsM = do
  c <- getCheckpoint
  ccBefore <- ccM
  tcs <- tcsM
  ccAfter <- ccM
  -- Unique symbols are either definitions or references. References
  -- share their unique index with at least one
  -- definition. Definitions should never share index with any other
  -- definition or we may have namespace collisions. `normalizeCode`
  -- will return nothing to signal that the latter assumption does
  -- not hold. If this happens it is a bug in the code generating
  -- the C++ AST, probably you used a definition symbol as a
  -- reference without casting it to a reference symbol. Use
  -- `symbolRef` to do that.
  --
  -- I can't think of a way to encode the above in the
  -- typesystem. Using a different type for definitions does not
  -- help because we need a correspondence between references and
  -- definitions, ie we need to index the definitions, ie we need to
  -- assign unique indexes.
  --
  tcsNorm <- maybe (throwError $ DuplicateSymbols $ toList tcs) return
    $ CC.normalizeCode tcs
  case tcsNorm `HM.lookup` ccBefore of
    Nothing -> do
      let cc' = HM.insert tcsNorm tcs ccAfter
      return (cc',tcs)
    Just tcs' -> do
      restoreCheckpoint c
      return (ccBefore,tcs')

getScopeQueries :: forall c e s m .
                  (Monad m, MonadReadScope c (QueryShape e s) m) =>
                  m (ScopeEnv c (QueryShape e s))
getScopeQueries = getScope
getQueries :: forall c e s m .
                (Monad m, Functor c, MonadReadScope c (QueryShape e s) m) =>
                m (c (QueryShape e s))
getQueries = fmap fst . runScopeEnv <$> getScopeQueries

-- | Use the queries as scope to run the computation.
evalQueryEnv
  :: forall c e s m a .
  (Traversable c,MonadSoftCodeBuilder m)
  => c (QueryShape e s)
  -> ReaderT (ScopeEnv c (QueryShape e s)) m a
  -> m a
evalQueryEnv queries funcBuilder = do
  x <- ScopeEnv <$> traverse go queries
  runReaderT funcBuilder x
  where
    go :: QueryShape e s -> m (QueryShape e s,CC.Symbol CC.CodeSymbol)
    go q = do
      sym <- mkUSymbol "record"
      return (q,sym)

evalAggrEnv :: (Monoid (ScopeEnv c AggrFunction), Monad m) =>
              StateT (ScopeEnv c AggrFunction) m a
            -> m a
evalAggrEnv = (`evalStateT` mempty)

runSoftCodeBuilder :: Monad m => StateT Int m a -> m a
runSoftCodeBuilder = (`evalStateT` 0)

type MonadCodeCheckpoint e s t n m =
  (Hashables2 e s
  ,CC.ExpressionLike e
  ,HasCallStack
  ,MonadCodeError e s t n m
  ,MonadCodeBuilder e s t n m
  ,MonadSoftCodeBuilder m
  ,MonadCheckpoint m)
