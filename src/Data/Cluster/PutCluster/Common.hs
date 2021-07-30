{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

module Data.Cluster.PutCluster.Common
  ( noopRef
  , withOpRef
  , withOpRefM
  , runQnfCache
  , idempotentClusterInsert
  , toNQNFQueryUC
  , toNQNFQueryBC
  , cachedMkRef
  , nqnfJoinC
  ) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bitraversable
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types
import           Data.Cluster.Types.Zip
import           Data.Functor.Identity
import qualified Data.HashSet               as HS
import           Data.Maybe
import           Data.NodeContainers
import           Data.Proxy
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.ListT
import           Data.Utils.Unsafe

noopRef :: Alternative f => NodeRef n -> WMetaD (f a) NodeRef n
noopRef r = WMetaD (empty, r)
-- XXX: it's not a column, it's a name
withOpRef :: (Hashables2 e s, Applicative f, Traversable op) =>
            op (PlanSym e s)
          -> NodeRef n
          -> WMetaD (f (op (PlanSym e s))) NodeRef n
withOpRef = withOpRefM . pure

withOpRefM :: (Hashables2 e s,Traversable op) =>
             f (op (PlanSym e s))
           -> NodeRef n
           -> WMetaD (f (op (PlanSym e s))) NodeRef n
withOpRefM op r = WMetaD (op, r)

putRet
  :: forall e s t n m f c .
  (Hashables2 e s
  ,Monad m
  ,SpecificCluster c
  ,Eq (c NodeRef f t n)
  ,CanPutIdentity (c NodeRef f) (c Identity Identity) NodeRef f
  ,Bitraversable (c Identity Identity)
  ,Zip2 (c Identity Identity)
  ,f ~ ComposedType c (PlanSym e s) NodeRef)
  => c NodeRef (ComposedType c (PlanSym e s) NodeRef) t n
  -> c NodeRef (ComposedType c (PlanSym e s) NodeRef) t n
  -> CGraphBuilderT e s t n m (c NodeRef f t n)
putRet ret c = do
  clust'' <- fmap dropIdentity
    $ bitraverse (return . fst) (uncurry combOps)
    $ zip2 (putIdentity ret)
    $ putIdentity c
  let retAny = fromSpecificClust ret
  let oldAny = fromSpecificClust clust''
  replaceCluster retAny oldAny
  return clust''
  where
    combOps :: ComposedType c (PlanSym e s) NodeRef n
            -> f n
            -> CGraphBuilderT e s t n m (f n)
    combOps l r = case (extractRef pc pe l,extractRef pc pe r) of
      ((opl,fl),(opr,fr)) -> if fl /= fr then return l -- It's an intermediate.
        else case mkComposedType pc pe (opl <|> opr,fl :: NodeRef n) of
          Just x  -> return x
          Nothing -> throwAStr "oops"
      where
        pc = Proxy :: Proxy c
        pe = Proxy :: Proxy (PlanSym e s)



-- | > idempotentClusterInsert
--   [(binClusterLeftIn,inRefL),
--    (binClusterRightIn,inRefR),
--    (binClusterOut,outRef)]
--   $ do ..
--
-- Will check if there are clusters with all the references at the
-- right places and will avoid running `do ..` if it finds anything.
idempotentClusterInsert
  :: forall e s t n m f c .
  (Hashables2 e s
  ,Monad m
  ,SpecificCluster c
  ,Eq (c NodeRef f t n)
  ,CanPutIdentity (c NodeRef f) (c Identity Identity) NodeRef f
  ,Bitraversable (c Identity Identity)
  ,Zip2 (c Identity Identity)
  ,Hashable (c NodeRef f t n)
  ,f ~ ComposedType c (PlanSym e s) NodeRef)
  => [(c NodeRef f t n
       -> f n
      ,NodeRef n)]
  -> CGraphBuilderT e s t n m (c NodeRef f t n)
  -> CGraphBuilderT e s t n m (c NodeRef f t n)
idempotentClusterInsert constraints m = do
  constrainedClusterSets <- forM constraints $ \(f,ref) -> do
    clusts <- lookupClustersN ref
    return
      $ HS.fromList
      $ filter ((ref ==) . snd . extrRef . f)
      $ mapMaybe toSpecificClustLocal clusts
  case constrainedClusterSets of
    [] -> m
    x:xs -> case HS.toList $ foldl HS.intersection x xs of
      []    -> m
      [ret] -> putRet ret =<< quarantine m
      _     -> throwAStr "Multiple equivalent clusters."
  where
    toSpecificClustLocal
      :: AnyCluster e s t n
      -> Maybe (c NodeRef (ComposedType c (PlanSym e s) NodeRef) t n)
    toSpecificClustLocal = toSpecificClust
    extrRef :: f n -> ([ClusterOp c (PlanSym e s)],NodeRef n)
    extrRef = extractRef (Proxy :: Proxy c) (Proxy :: Proxy (PlanSym e s))

-- The exceptions are real, the state is dropped.
quarantine
  :: Monad m => CGraphBuilderT e s t n m a -> CGraphBuilderT e s t n m a
quarantine m = do
  cqnf <- get
  gbst <- lift2 get
  lift3 ((`runStateT` gbst) $ (`runStateT` cqnf) $ runExceptT m) >>= \case
    ((Left e,_),_) -> throwError e
    ((Right r,cqnf'),_) -> do
      modify $ \cc -> cc{clustBuildCache=clustBuildCache cqnf'}
      return r

-- | Make a reference to a QNF unless the qnf is already there.
cachedMkRef
  :: (HasCallStack,Hashables2 e s,Monad m)
  => (c -> Maybe (NodeRef n))
  -> (NodeRef n -> c -> c)
  -> QNFQuery e s
  -> StateT c (CGraphBuilderT e s t n m) (NodeRef n)
cachedMkRef fromCache toCache qnf = gets fromCache >>= \case
  Nothing -> do
    x <- lift mkRef
    modify $ toCache x
    return x
  Just ref -> lift (linkNRefQnf ref qnf) >> return ref
  where
    mkRef = mkNodeFromQnf qnf >>= \case
      [ref] -> return ref
      refs
        -> throwAStr $ "Expected exactly one ref per qnf: " ++ ashow (refs,qnf)

runQnfCache
  :: Monad m => ListT (QNFBuild e s) a -> CGraphBuilderT e s t n m [a]
runQnfCache
  v = gets (runStateT (runListT v) . qnfBuildCache . clustBuildCache) >>= \case
  Left err -> throwAStr $ ashow err
  Right (x,c) -> do
    modify $ \cc -> cc
      { clustBuildCache = (clustBuildCache cc) { qnfBuildCache = c } }
    return x
nqnfJoinC
  :: (Monad m,Hashables2 e s) =>
  Prop (Rel (Expr e))
  -> NQNFQueryI e s
  -> NQNFQueryI e s
  -> CGraphBuilderT e s t n m
  [NQNFResultI (Prop (Rel (Expr (QNFName e s, e)))) e s]
nqnfJoinC p l r = runQnfCache $ nqnfJoin p l r
toNQNFQueryUC
  :: (Hashables2 e s,Monad m)
  => UQOp e
  -> NQNFQueryI e s
  -> CGraphBuilderT e s t n m (NQNFResultI (UQOp (QNFName e s,e)) e s)
toNQNFQueryUC o l = fmap headErr $ runQnfCache $ lift $ toNQNFQueryU o l
toNQNFQueryBC
  :: (Hashables2 e s,Monad m) =>
  BQOp e
  -> NQNFQueryI e s
  -> NQNFQueryI e s
  -> CGraphBuilderT e s t n m
  [NQNFResultI (Either (BQOp (QNFName e s, e)) (MkQB e s)) e s]
toNQNFQueryBC o l r = runQnfCache $ toNQNFQueryB o l r
