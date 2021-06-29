{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}

module Data.Cluster.PutCluster.Common
  ( noopRef
  , withOpRef
  , withOpRefM
  , runCnfCache
  , idempotentClusterInsert
  , toNCNFQueryUC
  , toNCNFQueryBC
  , cachedMkRef
  , ncnfJoinC
  ) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bitraversable
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types
import           Data.Cluster.Types.Zip
import           Data.CnfQuery.Build
import           Data.CnfQuery.Types
import           Data.Functor.Identity
import qualified Data.HashSet               as HS
import           Data.Maybe
import           Data.NodeContainers
import           Data.Proxy
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
  ccnf <- get
  gbst <- lift2 get
  lift3 ((`runStateT` gbst) $ (`runStateT` ccnf) $ runExceptT m) >>= \case
    ((Left e,_),_) -> throwError e
    ((Right r,ccnf'),_) -> do
      modify $ \cc -> cc{clustBuildCache=clustBuildCache ccnf'}
      return r

-- | Make a reference to a CNF unless the cnf is already there.
cachedMkRef
  :: (Hashables2 e s,Monad m)
  => (c -> Maybe (NodeRef n))
  -> (NodeRef n -> c -> c)
  -> CNFQuery e s
  -> StateT c (CGraphBuilderT e s t n m) (NodeRef n)
cachedMkRef fromCache toCache cnf = gets fromCache >>= \case
  Nothing -> do
    x <- lift mkRef
    modify $ toCache x
    return x
  Just ref -> lift (linkNRefCnf ref cnf) >> return ref
  where
    mkRef = mkNodeFromCnf cnf >>= \case
      [ref] -> return ref
      refs
        -> throwAStr $ "Expected exactly one ref per cnf: " ++ ashow (refs,cnf)

runCnfCache :: Monad m =>
              ListT (CNFBuild e s) a
            -> CGraphBuilderT e s t n m [a]
runCnfCache v =
  gets (runStateT (runListT v) . cnfBuildCache . clustBuildCache) >>= \case
    Left err -> throwAStr $ ashow err
    Right (x,c) -> do
      modify $ \cc -> cc{clustBuildCache=(clustBuildCache cc){cnfBuildCache=c}}
      return x
ncnfJoinC :: (Monad m,Hashables2 e s) =>
            Prop (Rel (Expr e))
          -> NCNFQueryI e s
          -> NCNFQueryI e s
          -> CGraphBuilderT e s t n m
          [NCNFResultI (Prop (Rel (Expr (CNFName e s, e)))) e s]
ncnfJoinC p l r = runCnfCache $ ncnfJoin p l r
toNCNFQueryUC :: (Hashables2 e s, Monad m) =>
                UQOp e
              -> NCNFQueryI e s
              -> CGraphBuilderT e s t n m
              (NCNFResultI (UQOp (CNFName e s, e)) e s)
toNCNFQueryUC o l = fmap headErr $ runCnfCache $ lift $ toNCNFQueryU o l
toNCNFQueryBC :: (Hashables2 e s,Monad m) =>
                BQOp e
              -> NCNFQueryI e s
              -> NCNFQueryI e s
              -> CGraphBuilderT e s t n m
              [NCNFResultI (Either (BQOp (CNFName e s, e)) (MkQB e s)) e s]
toNCNFQueryBC o l r = runCnfCache $ toNCNFQueryB o l r
