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

module Data.Cluster.InsertQuery (insertQueryPlan) where

import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.Free
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Cluster.ClusterConfig
import           Data.Cluster.PutCluster
import           Data.Cluster.PutCluster.Common
import           Data.Cluster.PutCluster.Join
import           Data.Cluster.Types
import           Data.CnfQuery.Build
import           Data.CnfQuery.BuildUtils
import           Data.CnfQuery.Types
import           Data.CppAst.CppType
import qualified Data.HashMap.Strict            as HM
import qualified Data.HashSet                   as HS
import qualified Data.List.NonEmpty             as NEL
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.Tup
import           Data.Utils.Unsafe

appendInsPlanRes :: Hashables2 e s =>
                   InsPlanRes e s t n
                 -> InsPlanRes e s t n
                 -> InsPlanRes e s t n
appendInsPlanRes l r = InsPlanRes {
  insPlanRef=insPlanRef l,
  insPlanNCNFs=insPlanNCNFs l <> insPlanNCNFs r,
  insPlanQuery=insPlanQuery l}

insertQueryPlan
  :: forall e s t n m .
  (Hashables2 e s,Monad m)
  => (e -> Maybe CppType)
  -> Free (Compose NEL.NonEmpty (Query e)) (s,QueryPlan e s)
  -> CGraphBuilderT e s t n m (NodeRef n)
insertQueryPlan litType = fmap insPlanRef . recur . freeToForest where
  recur :: QueryForest e s -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
  recur forest = do
    cachedM forest $ case qfQueries forest of
      Right s -> go0 s
      Left qs -> fmap foldInsPlanRes $ forM qs $ \case
        Q2 o l r -> go2 o (queryToForest l) (queryToForest r)
        Q1 o q   -> go1 o (queryToForest q)
        Q0 s     -> recur s
    where
      cachedM :: QueryForest e s
              -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
              -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
      cachedM forest' m = do
        gets (HM.lookup forest' . queriesCache . clustBuildCache) >>= \case
          Just ret -> return ret
          Nothing -> do
            ret <- m
            modify $ \cc -> cc{
              clustBuildCache=(clustBuildCache cc){
                  queriesCache=HM.insert forest' ret $ queriesCache $ clustBuildCache cc}}
            return ret
      mkS (e,c) = mkPlanSym (Column c 0) e
      go0 :: (s,QueryPlan e s)
          -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
      go0 (s,plan) = do
        let ncnfS :: NCNFQuery e s = ncnfSymbol (planSymOrig <$> planAllSyms plan) s
        ref <- mkNodeFromCnf (ncnfToCnf ncnfS) >>= \case
          [ref] -> return ref
          _     -> throwAStr $ "Multiple refs for symbol " ++ ashow s
        _ <- putNCluster plan (ref,ncnfS)
        return InsPlanRes {
          insPlanRef=ref,
          insPlanNCNFs=HS.singleton $ second putEmptyCNFQ ncnfS,
          insPlanQuery=Q0 (s,plan)}
      go1 :: UQOp e
          -> QueryForest e s
          -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
      go1 o q = fmap foldInsPlanRes $ (>>= toNEL) $ (`evalStateT` (Nothing,Nothing)) $ do
        iprQ :: InsPlanRes e s t n <- lift $ recur q
        forM (HS.toList $ insPlanNCNFs iprQ) $ \ncnfI -> do
          let refIn = (insPlanRef iprQ,
                       putIdentityCNFQ (fst <$> insPlanQuery iprQ) <$> ncnfI)
          (refO,assocO,opO) <- expandOp cachedMkRo ncnfI o
          let coOpM :: Maybe (UQOp e) = coUQOp o $ HM.keys $ fst ncnfI
          (refCoO,assocCoO,opCoOM) <- case coOpM of
            Just coOp -> expandOp cachedMkRcoo ncnfI coOp <&> \case
              (a,b,c) -> (a,b,Just c)
            Nothing   -> return (refIn,[],Nothing)
          void $ lift $ putUnCluster
            (assocO,assocCoO)
            litType
            (opO,opCoOM)
            refIn
            refCoO
            refO
          return InsPlanRes{
            insPlanRef=fst refO,
            insPlanNCNFs=HS.singleton $ second putEmptyCNFQ $ snd refO,
            insPlanQuery=Q1 o dbgQ}
          where
            dbgQ :: Query e (s, QueryPlan e s)
            dbgQ = forestToQuery q
            cachedMkRo = cachedMkRef snd $ second . const . Just
            cachedMkRcoo = cachedMkRef fst $ first . const . Just
            expandOp :: (CNFQuery e s ->
                        StateT
                         (Maybe (NodeRef n), Maybe (NodeRef n))
                         (CGraphBuilderT e s t n m)
                         (NodeRef n))
                     -> NCNFQueryI e s
                     -> UQOp e
                     -> StateT (Maybe (NodeRef n),Maybe (NodeRef n))
                     (CGraphBuilderT e s t n m)
                     ((NodeRef n, NCNFQuery e s),
                      [(PlanSym e s, PlanSym e s)],
                      UQOp (PlanSym e s))
            expandOp cachedMkR ncnfI o' = do
              resO <- lift $ toNCNFQueryUC o' ncnfI
              let ncnfO = putIdentityCNFQ (fst <$> Q1 o' dbgQ) <$> ncnfResNCNF resO
              refO <- cachedMkR (ncnfToCnf ncnfO)
              return ((refO,ncnfO),
                      bimap mkS mkS <$> ncnfResInOutNames resO,
                      uncurry mkPlanSym <$> ncnfResOrig resO)
      go2 :: BQOp e
          -> QueryForest e s
          -> QueryForest e s
          -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
      go2 o l r = case o of
        QProd -> throwAStr "We shouldn't be adding products..."
        QJoin p -> joinLike p qrefO
        QLeftAntijoin  p -> joinLike p qrefLO
        QRightAntijoin p -> joinLike p qrefRO
        _ -> do
          Tup2 iprL iprR <- recur `traverse` Tup2 l r
          let refL = insPlanRef iprL
          let refR = insPlanRef iprR
          let ncnfLR = (,)
                <$> HS.toList (insPlanNCNFs iprL)
                <*> HS.toList (insPlanNCNFs iprR)
          fmap foldInsPlanRes $ (>>= toNEL . join)
            $ forM ncnfLR $ \(ncnfL,ncnfR) -> do
              ress <- toNCNFQueryBC o ncnfL ncnfR
              forM ress $ \resO -> case ncnfResOrig resO of
                Right (MkQB mk) ->
                  recur $ queryToForest $ first snd $ mk l r
                Left (fmap (uncurry mkPlanSym) -> opO) -> do
                  let ncnfO = second (putIdentityCNFQ $ fst <$> dbgQ)
                              $ ncnfResNCNF resO
                      assoc = bimap mkS mkS <$> ncnfResInOutNames resO
                  refO <- fmap headErr $ mkNodeFromCnf $ ncnfToCnf ncnfO
                  void $ putBinCluster
                    assoc
                    opO
                    (refL,putQ dbgL ncnfL) (refR,putQ dbgR ncnfR) (refO,ncnfO)
                  return InsPlanRes{
                    insPlanRef=refO,
                    insPlanNCNFs=HS.singleton $ second putEmptyCNFQ ncnfO,
                    insPlanQuery=dbgQ}
        where
          dbgQ = Q2 o dbgL dbgR
          dbgL = forestToQuery l
          dbgR = forestToQuery r
          putQ = second . putIdentityCNFQ . fmap fst
          joinLike :: Prop (Rel (Expr e))
                   -> (JoinClustConfig n e s -> QRef n e s)
                   -> CGraphBuilderT e s t n m (InsPlanRes e s t n)
          joinLike p f = snd
                  <$> liftA2' (insertJoinLike mkExtr p) (recur l) (recur r)
            where
              liftA2' = join .... liftA2
              mkExtr conf = InsPlanRes {
                insPlanRef=getQRef $ f conf,
                insPlanNCNFs=HS.singleton $ second putEmptyCNFQ $ getNCNF $ f conf,
                insPlanQuery=dbgQ}

toNEL :: (MonadError err m, AShowError e s err) => [a] -> m (NEL.NonEmpty a)
toNEL = \case {[] -> throwAStr "NonEmpty nel"; x:xs -> return $ x NEL.:| xs}

insertJoinLike :: forall e s t n m . (Hashables2 e s, Monad m) =>
                 (JoinClustConfig n e s -> InsPlanRes e s t n)
               -> Prop (Rel (Expr e))
               -> InsPlanRes e s t n
               -> InsPlanRes e s t n
               -> CGraphBuilderT e s t n m (JoinClust e s t n,InsPlanRes e s t n)
insertJoinLike f p l r = do
  jcnfs <- mkJoinClustConfig p (toTriple l) (toTriple r)
  (clusts,ress) <- fmap unzip $ forM jcnfs $ \jcnf -> do
    clust <- putJoinClusterC jcnf
    return (clust,f jcnf)
  resNel <- toNEL ress
  clust NEL.:| _ <- toNEL clusts
  return (clust,foldInsPlanRes resNel)
  -- return $ nubOn (allNodeRefs (Proxy :: Proxy (PlanSym e s))) . reverse <$> x

foldInsPlanRes :: Hashables2 e s =>
                 NEL.NonEmpty (InsPlanRes e s t n) -> InsPlanRes e s t n
foldInsPlanRes (x NEL.:| xs) = foldr (\a b -> appendInsPlanRes a b) x xs
toTriple :: InsPlanRes e s t n -> (NodeRef n,[NCNFQueryI e s],Query e s)
toTriple InsPlanRes{..} =
  (insPlanRef,HS.toList insPlanNCNFs,fst <$> insPlanQuery)
