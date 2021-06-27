{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
module Data.Cluster.Propagators
  ( getReversibleB
  , getReversibleU
  , joinClustPropagator
  , binClustPropagator
  , unClustPropagator
  , cPropToACProp
  , putPlanPropagator
  , getPlanPropagators
  , getPlanCluster
  , putPlanCluster
  , getNodePlanFull
  , getNodePlan
  , delNodePlan
  , triggerClustPropagator
  , nClustPropagator
  , cPropToACPropN
  , modNodePlan
  , getValidClustPropagator
  , forceQueryPlan
  ) where

import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Trans.Maybe
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Bitraversable
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types
import           Data.Cluster.Types.Zip
import           Data.CppAst.CppType
import           Data.Either
import           Data.Functor.Identity
import qualified Data.HashMap.Strict                   as HM
import           Data.List
import qualified Data.List.NonEmpty                    as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.Optimizations.ExposeUnique
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.EmptyF
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.MTL
import           Data.Utils.Tup
import           Data.Utils.Types
import           Data.Utils.Unsafe

getReversibleB :: BQOp e -> IsReversible
getReversibleB = \case
  QProd            -> Reversible
  QJoin _          -> Irreversible
  QProjQuery       -> Irreversible
  QUnion           -> Reversible
  QDistinct        -> Irreversible
  QLeftAntijoin _  -> Irreversible
  QRightAntijoin _ -> Irreversible
getReversibleU :: UQOp e -> IsReversible
getReversibleU = \case
  QSel _     -> Reversible
  QGroup _ _ -> Irreversible
  QProj _    -> Reversible
  QSort _    -> Irreversible
  QLimit _   -> Reversible
  QDrop _    -> Reversible

asum :: [Defaulting a] -> Defaulting a
asum = foldr (<|>) empty

joinClustPropagator :: (HasCallStack, Hashables2 e s) =>
                      ([(PlanSym e s,PlanSym e s)],
                       [(PlanSym e s,PlanSym e s)],
                       [(PlanSym e s,PlanSym e s)])
                    -> Prop (Rel (Expr (PlanSym e s)))
                    -> CPropagatorPlan JoinClust' e s t n
joinClustPropagator (assocOL,assocO,assocOR) p JoinClust{joinBinCluster=BinClust{..},..} = do
  let il = s binClusterLeftIn
      ir = s binClusterRightIn
      o = s binClusterOut
      oL = s joinClusterLeftAntijoin
      oR = s joinClusterRightAntijoin
  oLRTrans <- translatePlan' (swap <$> assocOR) `traverse` oR
  oRRTrans <- translatePlan' (swap <$> assocOL) `traverse` oL
  iLTransOL <- translatePlan' assocOL `traverse` il
  iLTransO <- translatePlan' assocO `traverse` il
  -- Note that there may be different symbols, the rhs will be the latter ones.
  iRTransO <- translatePlan' (reverse assocO) `traverse` ir
  iRTransOR <- translatePlan' assocOR `traverse` ir
  outPlans :: Defaulting (QueryPlan e s) <- traverse
    (maybe (throwAStr $ "joinPlans couldn't get a plan:"
            ++ ashow (planSymEqs p,qpUnique <$> iLTransO,qpUnique <$> iRTransO))
     return)
    $ joinPlans (planSymEqs p) <$> iLTransO <*> iRTransO
  -- We need implement the directions that can be triggered
  let il' = asum [il,oLRTrans]
      ir' = asum [ir,oRRTrans]
      o' = asum [o,outPlans]
      oL' = asum [oL,iLTransOL]
      oR' = asum [oR,iRTransOR]
  return $ updateCHash JoinClust {
    joinBinCluster=updateCHash BinClust{
        binClusterT=EmptyF,
        binClusterLeftIn=c il' binClusterLeftIn,
        binClusterRightIn=c ir' binClusterRightIn,
        binClusterOut=c o' binClusterOut,
        binClusterHash=undefined},
    joinClusterLeftAntijoin=c oL' joinClusterLeftAntijoin,
    joinClusterRightAntijoin=c oR' joinClusterRightAntijoin,
    joinClusterLeftIntermediate=clrPlan joinClusterLeftIntermediate,
    joinClusterRightIntermediate=clrPlan joinClusterRightIntermediate,
    joinClusterLeftSplit=EmptyF,
    joinClusterRightSplit=EmptyF,
    joinClusterHash=undefined
  }
  where
    c :: Defaulting (QueryPlan e s)
      -> WMetaD (Defaulting (QueryPlan e s)) NodeRef n
      -> WMetaD (Defaulting (QueryPlan e s)) NodeRef n
    c v (WMetaD (_,n)) = WMetaD (v,n)
    s (WMetaD (v,_)) = v
    clrPlan r = WMetaD (empty, snd $ unMetaD r)

binClustPropagator :: Hashables2 e s =>
                     [(PlanSym e s,PlanSym e s)]
                   -> BQOp (PlanSym e s)
                   -> CPropagatorPlan BinClust' e s t n
binClustPropagator assoc o = mkBinPropagator mkOut mkIn mkIn
  where
    mkIn = G0 Nothing
    mkOut = bopOutput assoc o

unClustPropagator :: forall e s t n . (HasCallStack, Hashables2 e s) =>
                    Tup2 [(PlanSym e s, PlanSym e s)]
                  -> (e -> Maybe CppType)
                  -> UQOp (PlanSym e s)
                  -- -> QueryPlan e s
                  -> CPropagatorPlan UnClust' e s t n
unClustPropagator symAssocs literalType op = mkUnPropagator
  mkIn
  mkOutSec
  mkOutPrim
  where
    mkIn :: G e s (QueryPlan e s)
    mkIn = uopInput symAssocs op
    Tup2 mkOutPrim mkOutSec = uopOutputs symAssocs literalType op

nClustPropagator :: forall e s t n . Hashables2 e s =>
                   QueryPlan e s -> CPropagatorPlan NClust' e s t n
nClustPropagator p (NClust (WMetaD n)) =
  return $ NClust $ WMetaD $ first (const $ promoteDefaulting $ pure p) n


data G e s x = G0 (Maybe x)
  | GL (x -> Either (AShowStr e s) x)
  | GR (x -> Either (AShowStr e s) x)
  | G2 (x -> x -> Either (AShowStr e s) x)

argsG :: G e s x -> a -> a -> [a]
argsG = \case
  G0 _ -> \_ _ -> []
  GL _ -> \x _ -> [x]
  GR _ -> \_ x -> [x]
  G2 _ -> \x y -> [x,y]

-- | The first argument is the lower bound
promoteN :: Defaulting x -> Defaulting x -> Defaulting x
promoteN mark d = if mark `defaultingLe` d
                  then d
                  else promoteN mark $ promoteDefaulting d

-- | Combine two defaultings based on
liftG :: forall e s x .
        Defaulting x
      -> G e s x
      -> Defaulting x
      -> Defaulting x
      -> Either (AShowStr e s) (Defaulting x)
liftG old gf l r = promote <$> case gf of
  G0 Nothing  -> return old
  G0 (Just x) -> return $ pure x
  GL f        -> traverse f l
  GR f        -> traverse f r
  G2 f        -> sequenceA $ f <$> l <*> r
  where
    promote :: Defaulting x -> Defaulting x
    promote = promoteN (foldl (<|>) empty $ argsG gf l r) . (old <|>)
liftGC :: G e s x
       -- ^ The group transform
       -> WMetaD (Defaulting x) NodeRef n
       -- ^ Left input
       -> WMetaD (Defaulting x) NodeRef n
       -- ^ Right input
       -> WMetaD (Defaulting x) NodeRef n
       -- ^ Old output
       -> Either (AShowStr e s) (WMetaD (Defaulting x) NodeRef n)
liftGC f l r old =
  putMeta old <$> liftG (dropMeta old) f (dropMeta l) (dropMeta r)
  where
    dropMeta (WMetaD (d,_)) = d
    putMeta (WMetaD (_,n)) d = WMetaD (d,n)

mkUnPropagator :: forall e s t n . Hashables2 e s =>
                 G e s (QueryPlan e s)
               -> G e s (QueryPlan e s)
               -> G e s (QueryPlan e s)
               -> CPropagatorPlan UnClust' e s t n
mkUnPropagator fo1o2_i fio1_o2 fio2_o1 UnClust{..} = do
  unClusterPrimaryOut <-
      liftGC fio2_o1 unClusterIn unClusterSecondaryOut unClusterPrimaryOut
  unClusterSecondaryOut <-
      liftGC fio1_o2 unClusterIn unClusterPrimaryOut unClusterSecondaryOut
  unClusterIn <-
      liftGC fo1o2_i unClusterPrimaryOut unClusterSecondaryOut unClusterIn
  let unClusterT=EmptyF
  return UnClust {..}
mkBinPropagator :: forall e s t n . Hashables2 e s =>
                 G e s (QueryPlan e s)
               -> G e s (QueryPlan e s)
               -> G e s (QueryPlan e s)
               -> CPropagatorPlan BinClust' e s t n
mkBinPropagator fiLiR_o fiLo_iR fiRo_iL BinClust{..} = do
  binClusterOut <-
      liftGC fiLiR_o binClusterLeftIn binClusterRightIn binClusterOut
  binClusterLeftIn <-
      liftGC fiRo_iL binClusterRightIn binClusterOut binClusterLeftIn
  binClusterRightIn <-
      liftGC fiLo_iR binClusterLeftIn binClusterOut binClusterRightIn
  let binClusterT = EmptyF
  return BinClust {..}


-- get the group transform: Prim x Sec -> Inp
uopInput :: forall e s . (HasCallStack, Hashables2 e s) =>
           Tup2 [(PlanSym e s, PlanSym e s)]
         -> UQOp (PlanSym e s)
         -> G e s (QueryPlan e s)
uopInput (Tup2 assocPrim assocSec) op = case op of
  QSel _     -> pIn
  QGroup _ _ -> G0 Nothing
  QProj _    -> G0 Nothing
  QSort _    -> pIn
  QLimit _   -> pIn
  QDrop _    -> pIn
  where
    -- Just translate primary output
    pIn = GL $ \plan -> either
      (const $ throwAStr $ "[In]Lookup error: "
       ++ ashow (op,swap <$> assocPrim ++ assocSec,plan))
      return
      $ translatePlan' (swap <$> assocPrim ++ assocSec) plan
    swap (a,b) = (b,a)

-- get the group transform: Left x Right -> Out
bopOutput :: Hashables2 e s =>
            [(PlanSym e s,PlanSym e s)]
          -> BQOp (PlanSym e s)
          -> G e s (QueryPlan e s)
bopOutput assoc o = case o of
  QProd            -> G2 $ maybe (throwAStr "oops") (tp assoc) ... joinPlans []
  QJoin p          -> G2 $ maybe (throwAStr "oops") (tp assoc) ... joinPlans (planSymEqs p)
  QUnion           -> GL $ tp assoc
  QLeftAntijoin _  -> GL $ tp assoc
  QRightAntijoin _ -> GR $ tp (reverse assoc)
  QProjQuery       -> error "QProjQuery should have been optimized into a QProj"
  QDistinct        -> error "QDistinct should have been optimized into QGroup"
  where
    tp assoc' x = case translatePlan' assoc' x of
      Left _  -> throwAStr $ ashow (void o, length assoc)
      Right x -> return x

-- | Output is Tup2 (Inp x Sec -> Prim) (Inp x Prim -> Sec)
uopOutputs :: forall e s . (HasCallStack,Hashables2 e s) =>
             Tup2 [(PlanSym e s, PlanSym e s)]
             -- ^ Tup2 prim sec
           -> (e -> Maybe CppType)
           -> UQOp (PlanSym e s)
           -> Tup2 (G e s (QueryPlan e s))
uopOutputs (Tup2 assocPrim assocSec) literalType op = case op of
  QSel p     -> tw (>>= disambEq p)
  QGroup p es -> Tup2 (outGrpPlan p es) (G0 Nothing)
  QProj prj  -> Tup2 (projToPlan $ const prj) (projToPlan $ \p -> complementProj p prj)
  QSort _    -> Tup2 (GL $ pOut assocPrim) (G0 Nothing)
  QLimit _   -> tw id
  QDrop _    -> tw id
  where
    tw :: ([[PlanSym e s]] -> [[PlanSym e s]]) -> Tup2 (G e s (QueryPlan e s))
    tw modUniq = Tup2 (GL $ modUniqPlan <=< pOut assocPrim) (GL $ pOut assocSec)
      where
        modUniqPlan p = do
          uniq <- maybe
            (throwAStr "Empty list of uniques from disambiguation")
            return
            $ join
            $ traverse NEL.nonEmpty
            $ traverse NEL.nonEmpty
            $ modUniq
            $ fmap toList $ toList $ qpUnique p
          return p{qpUnique=uniq}
    pOut :: [(PlanSym e s, PlanSym e s)]
         -> QueryPlan e s -> Either (AShowStr e s) (QueryPlan e s)
    pOut assoc plan = either
      (const $ throwAStr $ "[Out]Lookup error: " ++ ashow (op,assoc,plan))
      return
      $ translatePlan' assoc plan
    outGrpPlan p es = GL $ \inp ->
      either (\e -> throwAStr $ "getQueryPlanGrp failed: " ++ ashow (e,op))
      return
      $ getQueryPlanGrp literalType p es inp
    projToPlan :: (QueryPlan e s -> [(PlanSym e s, Expr (PlanSym e s))])
               -> G e s (QueryPlan e s)
    projToPlan toPrj = GL go where
      go plan = either err return
                $ getQueryPlanPrj literalType prj plan
        where
          prj = toPrj plan
          err e = throwAStr $ "getQueryPlanPrj failed to infer proj schema: "
                  ++ ashow (prj,plan,e)


cPropToACPropN :: CPropagator a NClust' e s t n -> ACPropagator a e s t n
cPropToACPropN f = \case
  NClustW (NClust ref) ->
    NClustW . (\(NClust ref') -> NClust ref') <$> f (NClust ref)
  a -> return a
cPropToACProp :: forall c e s t n a ops .
                ((forall f g . CanPutIdentity (c f g) (c Identity Identity) f g),
                 ComposedType c (PlanSym e s) (WMetaD (Defaulting a) NodeRef)
                  ~ WMetaD ops (WMetaD (Defaulting a) NodeRef),
                  Zip2 (c Identity Identity),
                  SpecificCluster c) =>
                CPropagator a c e s t n -> ACPropagator a e s t n
cPropToACProp f ac = maybe (return ac) go $ toSpecificClust ac where
  go :: c
       (WMetaD (Defaulting a) NodeRef)
       (ComposedType c (PlanSym e s) (WMetaD (Defaulting a) NodeRef))
       t n
     -> Either (AShowStr e s) (PropCluster a NodeRef e s t n)
  go c = fromSpecificClust @c
    . dropId
    . bimap fst WMetaD
    . zip2 (fst cPair)
    . putId
    <$> f (dropId (snd cPair))
    where
      cPair = unzip2 $ bimap (,EmptyF) unMetaD $ putId c
      putId :: forall f g . c f g t n -> c Identity Identity (f t) (g n)
      putId = putIdentity
      dropId :: forall f g . c Identity Identity (f t) (g n) -> c f g t n
      dropId = dropIdentity

putPlanPropagator :: (Hashables2 e s, Monad m) =>
                    AnyCluster e s t n
                  -> (ACPropagator (QueryPlan e s) e s t n,
                     [(PlanSym e s,PlanSym e s)])
                  -- ^(Propagator,In/Out mapping)
                  -> CGraphBuilderT e s t n m ()
putPlanPropagator c p = modPropagators c
    $ Just . (\cp -> cp{planPropagators=p `ins` planPropagators cp}) . fromMaybe def
  where
    -- Note: we insert unique io assocs. The reason is that the assoc
    -- will have the same symbols semantically, only different
    -- provenance. However in case of projection and grouping the
    -- association will be empty and we have no way of telling the
    -- propagators apart.
    ins a@(_,[]) as = a:as
    ins a@(_,k) as  = if k `notElem` fmap snd as then a:as else as
modPropagators :: (Hashables2 e s, Monad m) =>
                 AnyCluster e s t n
               -> Endo (Maybe (ClustPropagators e s t n))
               -- ^(Propagator,In/Out mapping)
               -> CGraphBuilderT e s t n m ()
modPropagators c f = modify $ \clustConf -> clustConf{
  cnfPropagators=HM.alter f c $ cnfPropagators clustConf}
getPropagators :: (Hashables2 e s, MonadReader (ClusterConfig e s t n) m) =>
                 AnyCluster e s t n
               -> m (Maybe (ClustPropagators e s t n))
getPropagators c = asks (HM.lookup c . cnfPropagators)

getPlanPropagators :: (Hashables2 e s, MonadReader (ClusterConfig e s t n) m) =>
                     AnyCluster e s t n
                   -> m [(ACPropagator (QueryPlan e s) e s t n,
                         [(PlanSym e s, PlanSym e s)])]
getPlanPropagators c = maybe [] planPropagators <$> getPropagators c

getNodePlan :: MonadReader (ClusterConfig e s t n) m =>
              NodeRef n -> m (Defaulting (QueryPlan e s))
getNodePlan nref = asks (fromMaybe empty . refLU nref . cnfNodePlans)

modNodePlan :: MonadState (ClusterConfig e s t n) m =>
              NodeRef n
            -> Endo (Defaulting (QueryPlan e s))
            -> m ()
modNodePlan nref f = modify $ \clustConf -> clustConf {
  cnfNodePlans=refAlter
    (Just . f . fromMaybe empty)
    nref
    $ cnfNodePlans clustConf}
delNodePlan :: Monad m => NodeRef n -> CGraphBuilderT e s t n m ()
delNodePlan nref = modify $ \clustConf -> clustConf {
  cnfNodePlans=refAdjust demoteDefaulting nref $ cnfNodePlans clustConf}

-- | Fill the noderefs with plans.
getPlanCluster :: forall e s t n m .
                 (MonadReader (ClusterConfig e s t n) m, Hashables2 e s) =>
                 AnyCluster e s t n
               -> m (PlanCluster NodeRef e s t n)
               -- -> CGraphBuilderT e s t n m (PlanCluster NodeRef e s t n)
getPlanCluster = fmap dropId
                 . bitraverse (withComp $ const $ return empty)
                 (withComp getNodePlan)
                 . putId
  where
    withComp :: (NodeRef a -> m x)
             -> NodeRef a
             -> m (WMetaD x NodeRef a)
    withComp f = fmap WMetaD . (\t -> (,t) <$> f t)
    putId :: AnyCluster e s t n
          -> AnyCluster' (PlanSym e s) Identity (NodeRef t) (NodeRef n)
    putId = putIdentity
    dropId :: AnyCluster' (PlanSym e s) Identity
             (WMetaD (Defaulting (QueryPlan e s)) NodeRef t)
             (WMetaD (Defaulting (QueryPlan e s)) NodeRef n)
           -> PlanCluster NodeRef e s t n
    dropId = dropIdentity

putPlanCluster :: forall e s t n m .
                 (MonadState (ClusterConfig e s t n) m, Hashables2 e s) =>
                 PlanCluster NodeRef e s t n
               -> m ()
putPlanCluster = void . traverse (uncurry go . unMetaD) . putId
  where
    go :: Defaulting (QueryPlan e s) -> NodeRef n -> m ()
    go p ref = ifDefaultingEmpty p (return ()) $ modNodePlan ref $ const p
    putId :: PlanCluster NodeRef e s t n
          -> AnyCluster'
            (PlanSym e s)
            Identity
            (WMetaD (Defaulting (QueryPlan e s)) NodeRef t)
            (WMetaD (Defaulting (QueryPlan e s)) NodeRef n)
    putId = putIdentity


getValidClustPropagator :: (MonadReader (ClusterConfig e s t n) m,
                           MonadError err m, AShowError e s err,
                           Hashables2 e s, HasCallStack) =>
                          AnyCluster e s t n
                        -> m
                        ((ACPropagator (QueryPlan e s) e s t n,
                          [(PlanSym e s, PlanSym e s)]),
                         PlanCluster NodeRef e s t n)
getValidClustPropagator clust = do
  propsAssoc <- getPlanPropagators clust
  curPlanCluster <- getPlanCluster clust
  -- `assc` is empty..
  let planClusters = [(prop curPlanCluster,(prop,assc))
                     | (prop,assc) <- propsAssoc]
  case find (isRight . fst) planClusters of
    Just (Right newPlanClust,propAssc) -> return (propAssc,newPlanClust)
    Nothing ->
      throwAStr $ "Expected at least one good plan cluster (some errors are ok): "
      ++ ashow (clust,second (first $ const (Sym "<the prop>")) <$> planClusters)
    _ -> undefined

triggerClustPropagator :: (MonadState (ClusterConfig e s t n) m,
                          MonadError err m, AShowError e s err,
                          Hashables2 e s) =>
                          AnyCluster e s t n -> m ()
triggerClustPropagator clust = do
  (_,newPlanClust) <- dropReader get $ getValidClustPropagator clust
  putPlanCluster newPlanClust
getNodePlanFull :: MonadReader (ClusterConfig e s t n) m =>
                  NodeRef n -> m (Maybe (QueryPlan e s))
getNodePlanFull r = asks $ getDefaultingFull <=< refLU r . cnfNodePlans

-- | Assume a consistent view of clusters. Find a trigger that will
-- return a query plen.
--
-- XXX: this fails when called on the bottom
forceQueryPlan :: forall e s t n m err .
                 (MonadState (ClusterConfig e s t n) m,
                  MonadError err m, AShowError e s err,
                  Hashables2 e s) =>
                 NodeRef n -> m (Maybe (QueryPlan e s))
forceQueryPlan n =
  runMaybeT
  $ (`evalStateT` mempty)
  $ go n
  where
    go :: NodeRef n -> StateT (NodeSet n) (MaybeT m) (QueryPlan e s)
    go ref = unlessDone $ do
      when (n == 6) $ traceM $ "go " ++ show ref
      trail <- get
      guard $ not $ ref `nsMember` trail
      modify (nsInsert ref)
      -- Here we actually need `eitherl`...
      clusts <- lift2 $ filter (elem ref . clusterOutputs) . (>>= snd)
               <$> lookupClustersN ref
      oneOfM clusts $ \c -> do
        case partition (elem ref) [clusterInputs c,clusterOutputs c] of
          ([_siblings],[deps]) -> do
            guard $ not $ any (`nsMember` trail) deps
            mapM_ go deps
            lift2$ triggerClustPropagator c
            unlessDone $ throwAStr "We made the deps "
          _ -> mzero
      where
        oneOfM [] _  = mzero
        oneOfM cs fm = foldr1Unsafe (<|>) $ fm <$> cs
        unlessDone :: StateT (NodeSet n) (MaybeT m) (QueryPlan e s)
                   -> StateT (NodeSet n) (MaybeT m) (QueryPlan e s)
        unlessDone m = dropReader (lift2 get) (getNodePlanFull ref) >>= \case
          Just x  -> return x
          Nothing -> m
