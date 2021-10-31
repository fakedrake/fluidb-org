{-# LANGUAGE CPP                  #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TupleSections        #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns         #-}

module Data.Cluster.InsertQuery (insertQueryForest) where

import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.State
import           Control.Utils.Free
import           Data.Bifunctor
import           Data.Cluster.ClusterConfig
import           Data.Cluster.PutCluster
import           Data.Cluster.PutCluster.Common
import           Data.Cluster.PutCluster.Join
import           Data.Cluster.Types
import           Data.Cluster.Types.Monad
import           Data.CppAst.CppType
import qualified Data.HashMap.Strict            as HM
import qualified Data.HashSet                   as HS
import qualified Data.IntMap                    as IM
import qualified Data.List.NonEmpty             as NEL
import           Data.NodeContainers
import           Data.QnfQuery.Build
import           Data.QnfQuery.BuildUtils
import           Data.QnfQuery.HashBag
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.Optimizations.Echo
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.Tup
import           Data.Utils.Unsafe

appendInsPlanRes
  :: Hashables2 e s
  => InsPlanRes e s t n
  -> InsPlanRes e s t n
  -> InsPlanRes e s t n
appendInsPlanRes l r = InsPlanRes {
  insPlanRef=insPlanRef l,
  insPlanNQNFs=insPlanNQNFs l <> insPlanNQNFs r,
  insPlanQuery=insPlanQuery l}

registerEcho :: Monad m => NodeRef n -> EchoSide -> CGraphBuilderT e s t n m ()
registerEcho n = \case
  NoEcho -> return ()
  TEntry i -> modify $ \cgs -> cgs
    { qnfTunels = IM.adjust (\tun -> tun { tEntrys = n : tEntrys tun }) i
        $ qnfTunels cgs
    }
  TExit i -> modify $ \cgs -> cgs
    { qnfTunels = IM.adjust (\tun -> tun { tExits = n : tExits tun }) i
        $ qnfTunels cgs
    }

insertQueryForest
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => (e -> Maybe CppType)
  -> Free (Compose NEL.NonEmpty (TQuery e)) (s,QueryShape e s)
  -> CGraphBuilderT e s t n m (NodeRef n)
insertQueryForest
  litType = fmap insPlanRef . insertQueryForest' litType . freeToForest

mkS :: (e,QNFColSPC HS.HashSet HashBag Either e s) -> ShapeSym e s
mkS (e,c) = mkShapeSym (Column c 0) e

-- |Insert a symbol.
insertQueryForest0
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => (s,QueryShape e s)
  -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
insertQueryForest0 (s,shape) = do
  let nqnfS :: NQNFQuery e s = nqnfSymbol (shapeSymOrig <$> shapeAllSyms shape) s
  ref <- mkNodeFromQnf (nqnfToQnf nqnfS) >>= \case
    [ref] -> return ref
    _     -> throwAStr $ "Multiple refs for symbol " ++ ashow s
  _ <- putNCluster shape (ref,nqnfS)
  return
    InsPlanRes
    { insPlanRef = ref
     ,insPlanNQNFs = HS.singleton $ second putEmptyQNFQ nqnfS
     ,insPlanQuery = Q0 (s,shape)
    }

insertQueryForest'
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => (e -> Maybe CppType)
  -> QueryForest e s
  -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
insertQueryForest' litType forest = do
  cachedM forest $ case qfQueries forest of
    Right s -> insertQueryForest0 s
    Left qs -> foldInsPlanRes <$> forM qs recurTQ
  where
    q2f = queryToForest . fmap tqueryToForest
    recurTQ q0 = do
      res <- case tqQuery q0 of
        Left q -> case q of
          Q1 o q'  -> insertQueryForest1 litType o $ q2f q'
          Q2 o l r -> insertQueryForest2 litType o (q2f l) (q2f r)
          Q0 q'    -> insertQueryForest' litType $ tqueryToForest q'
        Right x -> case x of
          Q1 o q   -> insertQueryForest1 litType o $ queryToForest q
          Q2 o l r -> insertQueryForest2 litType o (queryToForest l) (queryToForest r)
          Q0 q     -> insertQueryForest' litType q
      registerEcho (insPlanRef res) (tqEcho q0)
      return res
    cachedM :: QueryForest e s
            -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
            -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
    cachedM forest' m = do
      gets (HM.lookup forest' . queriesCache . clustBuildCache) >>= \case
        Just ret -> return ret
        Nothing -> do
          ret <- m
          modify $ \cc -> cc
            { clustBuildCache = (clustBuildCache cc)
                { queriesCache =
                    HM.insert forest' ret $ queriesCache $ clustBuildCache cc
                }
            }
          return ret

insertQueryForest2
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => (e -> Maybe CppType)
  -> BQOp e
  -> QueryForest e s
  -> QueryForest e s
  -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
insertQueryForest2 litType o l r = case o of
  QProd -> throwAStr "We shouldn't be adding products..."
  QJoin p -> joinLike p qrefO
  QLeftAntijoin p -> joinLike p qrefLO
  QRightAntijoin p -> joinLike p qrefRO
  _ -> do
    Tup2 iprL iprR <- insertQueryForest' litType `traverse` Tup2 l r
    let refL = insPlanRef iprL
    let refR = insPlanRef iprR
    let nqnfLR =
          (,) <$> HS.toList (insPlanNQNFs iprL)
          <*> HS.toList (insPlanNQNFs iprR)
    fmap foldInsPlanRes $ (>>= toNEL . join) $ forM nqnfLR $ \(nqnfL,nqnfR) -> do
      ress <- toNQNFQueryBC o nqnfL nqnfR
      forM ress $ \resO -> case nqnfResOrig resO of
        Right (MkQB mk)
          -> insertQueryForest' litType $ queryToForest $ first snd $ mk l r
        Left (fmap (uncurry mkShapeSym) -> opO) -> do
          let nqnfO =
                second (putIdentityQNFQ $ fst <$> dbgQ) $ nqnfResNQNF resO
              assoc = bimap mkS mkS <$> nqnfResInOutNames resO
          refO <- fmap headErr $ mkNodeFromQnf $ nqnfToQnf nqnfO
          void
            $ putBinCluster
              assoc
              opO
              (refL,putQ dbgL nqnfL)
              (refR,putQ dbgR nqnfR)
              (refO,nqnfO)
          return
            InsPlanRes
            { insPlanRef = refO
             ,insPlanNQNFs = HS.singleton $ second putEmptyQNFQ nqnfO
             ,insPlanQuery = dbgQ
            }
  where
    dbgQ = Q2 o dbgL dbgR
    dbgL = forestToQuery l
    dbgR = forestToQuery r
    putQ = second . putIdentityQNFQ . fmap fst
    joinLike :: Prop (Rel (Expr e))
             -> (JoinClustConfig n e s -> QRef n e s)
             -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
    joinLike p f =
      snd
      <$> liftA2'
        (insertJoinLike mkExtr p)
        (insertQueryForest' litType l)
        (insertQueryForest' litType r)
      where
        liftA2' = join .... liftA2
        mkExtr conf =
          InsPlanRes
          { insPlanRef = getQRef $ f conf
           ,insPlanNQNFs =
              HS.singleton $ second putEmptyQNFQ $ getNQNF $ f conf
           ,insPlanQuery = dbgQ
          }

-- | Inserting the in
--
-- We use a cache, the (Maybe outRef, Maybe coOutRef)
insertQueryForest1
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => (e -> Maybe CppType)
  -> UQOp e
  -> QueryForest e s
  -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
insertQueryForest1
  litType
  o
  q = fmap foldInsPlanRes $ (>>= toNEL) $ (`evalStateT` (Nothing,Nothing)) $ do
  -- insert the input query.
  iprQ :: InsPlanRes e s t n <- lift $ insertQueryForest' litType q
  -- For each possible nqnf that the input can correspond to, insert a
  -- query on top of it. Remember that we get multiple NQNFs because
  -- producs are non-deterministic.
  forM (HS.toList $ insPlanNQNFs iprQ) $ \nqnfI -> do
    let refIn =
          (insPlanRef iprQ
          ,putIdentityQNFQ (fst <$> insPlanQuery iprQ) <$> nqnfI)
    (refO,assocO,opO) <- expandOp cachedMkRo nqnfI o
    -- Get the complement. QSort and QGroup do not have complements.
    --
    -- XXX: the complement of the projection does not include the
    -- unique keys.
    let coOpM :: Maybe (UQOp e) = coUQOp o $ HM.keys $ fst nqnfI
    (refCoO,assocCoO,opCoOM) <- case coOpM of
      Just
        coOp -> expandOp cachedMkRcoo nqnfI coOp <&> \(a,b,c) -> (a,b,Just c)
      Nothing -> return (refIn,[],Nothing)
    void
      $ lift
      $ putUnCluster (assocO,assocCoO) litType (opO,opCoOM) refIn refCoO refO
    return
      InsPlanRes
      { insPlanRef = fst refO
       ,insPlanNQNFs = HS.singleton $ second putEmptyQNFQ $ snd refO
       ,insPlanQuery = Q1 o dbgQ
      }
  where
    dbgQ :: Query e (s,QueryShape e s)
    dbgQ = forestToQuery q
    cachedMkRo
      :: HasCallStack
      => QNFQuery e s
      -> StateT (a,Maybe (NodeRef n)) (CGraphBuilderT e s t n m) (NodeRef n)
    cachedMkRo qnf = do
      cachedMkRef snd (second . const . Just) qnf
    cachedMkRcoo
      :: QNFQuery e s
      -> StateT (Maybe (NodeRef n),a) (CGraphBuilderT e s t n m) (NodeRef n)
    cachedMkRcoo qnf = do
      cachedMkRef fst (first . const . Just) qnf
    -- | Calculate the output and insert it.
    expandOp
      :: (QNFQuery e s
          -> StateT
            (Maybe (NodeRef n),Maybe (NodeRef n))
            (CGraphBuilderT e s t n m)
            (NodeRef n))
      -> NQNFQueryI e s
      -> UQOp e
      -> StateT
        (Maybe (NodeRef n),Maybe (NodeRef n))
        (CGraphBuilderT e s t n m)
        ((NodeRef n,NQNFQuery e s)
        ,[(ShapeSym e s,ShapeSym e s)]
        ,UQOp (ShapeSym e s))
    expandOp cachedMkR nqnfI o' = do
      resO <- lift $ toNQNFQueryUC o' nqnfI
      let nqnfO = putIdentityQNFQ (fst <$> Q1 o' dbgQ) <$> nqnfResNQNF resO
      refO <- cachedMkR $ nqnfToQnf nqnfO
      return
        ((refO,nqnfO)
        ,bimap mkS mkS <$> nqnfResInOutNames resO
        ,uncurry mkShapeSym <$> nqnfResOrig resO)

toNEL :: (MonadError err m, AShowError e s err) => [a] -> m (NEL.NonEmpty a)
toNEL = \case
  []   -> throwAStr "NonEmpty nel"
  x:xs -> return $ x NEL.:| xs

insertJoinLike :: forall e s t n m . (Hashables2 e s, Monad m) =>
                 (JoinClustConfig n e s -> InsPlanRes e s t n)
               -> Prop (Rel (Expr e))
               -> InsPlanRes e s t n
               -> InsPlanRes e s t n
               -> CGraphBuilderT e s t n m (JoinClust e s t n,InsPlanRes e s t n)
insertJoinLike f p l r = do
  jqnfs <- mkJoinClustConfig p (toTriple l) (toTriple r)
  (clusts,ress) <- fmap unzip $ forM jqnfs $ \jqnf -> do
    clust <- putJoinClusterC jqnf
    return (clust,f jqnf)
  resNel <- toNEL ress
  clust NEL.:| _ <- toNEL clusts
  return (clust,foldInsPlanRes resNel)
  -- return $ nubOn (allNodeRefs (Proxy :: Proxy (ShapeSym e s))) . reverse <$> x

foldInsPlanRes :: Hashables2 e s =>
                 NEL.NonEmpty (InsPlanRes e s t n) -> InsPlanRes e s t n
foldInsPlanRes (x NEL.:| xs) = foldr appendInsPlanRes x xs
toTriple :: InsPlanRes e s t n -> (NodeRef n,[NQNFQueryI e s],Query e s)
toTriple InsPlanRes{..} =
  (insPlanRef,HS.toList insPlanNQNFs,fst <$> insPlanQuery)
