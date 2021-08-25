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
  , putShapePropagator
  , getShapePropagators
  , getShapeCluster
  , putShapeCluster
  , getNodeShapeFull
  , getNodeShape
  , delNodeShape
  , triggerClustPropagator
  , nClustPropagator
  , cPropToACPropN
  , modNodeShape
  , getValidClustPropagator
  , forceQueryShape
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
                      ([(ShapeSym e s,ShapeSym e s)],
                       [(ShapeSym e s,ShapeSym e s)],
                       [(ShapeSym e s,ShapeSym e s)])
                    -> Prop (Rel (Expr (ShapeSym e s)))
                    -> CPropagatorShape JoinClust' e s t n
joinClustPropagator (assocOL,assocO,assocOR) p JoinClust{joinBinCluster=BinClust{..},..} = do
  let il = s binClusterLeftIn
      ir = s binClusterRightIn
      o = s binClusterOut
      oL = s joinClusterLeftAntijoin
      oR = s joinClusterRightAntijoin
  oLRTrans <- translateShape' (swap <$> assocOR) `traverse` oR
  oRRTrans <- translateShape' (swap <$> assocOL) `traverse` oL
  iLTransOL <- translateShape' assocOL `traverse` il
  iLTransO <- translateShape' assocO `traverse` il
  -- Note that there may be different symbols, the rhs will be the latter ones.
  iRTransO <- translateShape' (reverse assocO) `traverse` ir
  iRTransOR <- translateShape' assocOR `traverse` ir
  outShapes :: Defaulting (QueryShape e s) <- traverse
    (maybe (throwAStr $ "joinShapes couldn't get a shape:"
            ++ ashow (shapeSymEqs p,qpUnique <$> iLTransO,qpUnique <$> iRTransO))
     return)
    $ joinShapes (shapeSymEqs p) <$> iLTransO <*> iRTransO
  -- We need implement the directions that can be triggered
  let il' = asum [il,oLRTrans]
      ir' = asum [ir,oRRTrans]
      o' = asum [o,outShapes]
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
    joinClusterLeftIntermediate=clrShape joinClusterLeftIntermediate,
    joinClusterRightIntermediate=clrShape joinClusterRightIntermediate,
    joinClusterLeftSplit=EmptyF,
    joinClusterRightSplit=EmptyF,
    joinClusterHash=undefined
  }
  where
    c :: Defaulting (QueryShape e s)
      -> WMetaD (Defaulting (QueryShape e s)) NodeRef n
      -> WMetaD (Defaulting (QueryShape e s)) NodeRef n
    c v (WMetaD (_,n)) = WMetaD (v,n)
    s (WMetaD (v,_)) = v
    clrShape r = WMetaD (empty, snd $ unMetaD r)

binClustPropagator :: Hashables2 e s =>
                     [(ShapeSym e s,ShapeSym e s)]
                   -> BQOp (ShapeSym e s)
                   -> CPropagatorShape BinClust' e s t n
binClustPropagator assoc o = mkBinPropagator mkOut mkIn mkIn
  where
    mkIn = G0 Nothing
    mkOut = bopOutput assoc o

unClustPropagator :: forall e s t n . (HasCallStack, Hashables2 e s) =>
                    Tup2 [(ShapeSym e s, ShapeSym e s)]
                  -> (e -> Maybe CppType)
                  -> UQOp (ShapeSym e s)
                  -- -> QueryShape e s
                  -> CPropagatorShape UnClust' e s t n
unClustPropagator symAssocs literalType op = mkUnPropagator
  mkIn
  mkOutSec
  mkOutPrim
  where
    mkIn :: G e s (QueryShape e s)
    mkIn = uopInput symAssocs op
    Tup2 mkOutPrim mkOutSec = uopOutputs symAssocs literalType op

nClustPropagator :: forall e s t n . Hashables2 e s =>
                   QueryShape e s -> CPropagatorShape NClust' e s t n
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
                 G e s (QueryShape e s)
               -> G e s (QueryShape e s)
               -> G e s (QueryShape e s)
               -> CPropagatorShape UnClust' e s t n
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
                 G e s (QueryShape e s)
               -> G e s (QueryShape e s)
               -> G e s (QueryShape e s)
               -> CPropagatorShape BinClust' e s t n
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
           Tup2 [(ShapeSym e s, ShapeSym e s)]
         -> UQOp (ShapeSym e s)
         -> G e s (QueryShape e s)
uopInput (Tup2 assocPrim assocSec) op = case op of
  QSel _     -> pIn
  QGroup _ _ -> G0 Nothing
  QProj _    -> G0 Nothing
  QSort _    -> pIn
  QLimit _   -> pIn
  QDrop _    -> pIn
  where
    -- Just translate primary output
    pIn = GL $ \shape -> either
      (const $ throwAStr $ "[In]Lookup error: "
       ++ ashow (op,swap <$> assocPrim ++ assocSec,shape))
      return
      $ translateShape' (swap <$> assocPrim ++ assocSec) shape
    swap (a,b) = (b,a)

-- get the group transform: Left x Right -> Out
bopOutput :: Hashables2 e s =>
            [(ShapeSym e s,ShapeSym e s)]
          -> BQOp (ShapeSym e s)
          -> G e s (QueryShape e s)
bopOutput assoc o = case o of
  QProd            -> G2 $ maybe (throwAStr "oops") (tp assoc) ... joinShapes []
  QJoin p          -> G2 $ maybe (throwAStr "oops") (tp assoc) ... joinShapes (shapeSymEqs p)
  QUnion           -> GL $ tp assoc
  QLeftAntijoin _  -> GL $ tp assoc
  QRightAntijoin _ -> GR $ tp (reverse assoc)
  QProjQuery       -> error "QProjQuery should have been optimized into a QProj"
  QDistinct        -> error "QDistinct should have been optimized into QGroup"
  where
    tp assoc' x = case translateShape' assoc' x of
      Left _  -> throwAStr $ ashow (void o, length assoc)
      Right x -> return x

-- | Output is Tup2 (Inp x Sec -> Prim) (Inp x Prim -> Sec)
uopOutputs :: forall e s . (HasCallStack,Hashables2 e s) =>
             Tup2 [(ShapeSym e s, ShapeSym e s)]
             -- ^ Tup2 prim sec
           -> (e -> Maybe CppType)
           -> UQOp (ShapeSym e s)
           -> Tup2 (G e s (QueryShape e s))
uopOutputs (Tup2 assocPrim assocSec) literalType op = case op of
  QSel p -> tw (>>= disambEq p)
  QGroup p es -> Tup2 (outGrpShape p es) (G0 Nothing)
  QProj prj -> Tup2
    (projToShape $ const prj)
    (projToShape $ \p -> complementProj p prj)
  QSort _ -> Tup2 (GL $ pOut assocPrim) (G0 Nothing)
  QLimit _ -> tw id
  QDrop _ -> tw id
  where
    tw :: ([[ShapeSym e s]] -> [[ShapeSym e s]])
       -> Tup2 (G e s (QueryShape e s))
    tw
      modUniq = Tup2 (GL $ modUniqShape <=< pOut assocPrim) (GL $ pOut assocSec)
      where
        modUniqShape p = do
          uniq <- maybe
            (throwAStr "Empty list of uniques from disambiguation")
            return
            $ join
            $ traverse NEL.nonEmpty
            $ traverse NEL.nonEmpty
            $ modUniq
            $ fmap toList
            $ toList
            $ qpUnique p
          return p { qpUnique = uniq }
    pOut :: [(ShapeSym e s,ShapeSym e s)]
         -> QueryShape e s
         -> Either (AShowStr e s) (QueryShape e s)
    pOut assoc shape =
      either
        (const $ throwAStr $ "[Out]Lookup error: " ++ ashow (op,assoc,shape))
        return
      $ translateShape' assoc shape
    outGrpShape p es = GL $ \inp -> either
      (\e -> throwAStr $ "getQueryShapeGrp failed: " ++ ashow (e,op))
      return
      $ getQueryShapeGrp literalType p es inp
    projToShape :: (QueryShape e s -> [(ShapeSym e s,Expr (ShapeSym e s))])
                -> G e s (QueryShape e s)
    projToShape toPrj = GL go
      where
        go shape = either err return $ getQueryShapePrj literalType prj shape
          where
            prj = toPrj shape
            err e =
              throwAStr
              $ "getQueryShapePrj failed to infer proj schema: "
              ++ ashow (prj,shape,e)

cPropToACPropN :: CPropagator a NClust' e s t n -> ACPropagator a e s t n
cPropToACPropN f = \case
  NClustW (NClust ref) ->
    NClustW . (\(NClust ref') -> NClust ref') <$> f (NClust ref)
  a -> return a

cPropToACProp :: forall c e s t n a ops .
                ((forall f g . CanPutIdentity (c f g) (c Identity Identity) f g),
                 ComposedType c (ShapeSym e s) (WMetaD (Defaulting a) NodeRef)
                  ~ WMetaD ops (WMetaD (Defaulting a) NodeRef),
                  Zip2 (c Identity Identity),
                  SpecificCluster c) =>
                CPropagator a c e s t n -> ACPropagator a e s t n
cPropToACProp f ac = maybe (return ac) go $ toSpecificClust ac where
  go :: c
       (WMetaD (Defaulting a) NodeRef)
       (ComposedType c (ShapeSym e s) (WMetaD (Defaulting a) NodeRef))
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

putShapePropagator
  :: (Hashables2 e s,Monad m)
  => AnyCluster e s t n
  -> (ACPropagator (QueryShape e s) e s t n,[(ShapeSym e s,ShapeSym e s)])
  -- ^(Propagator,In/Out mapping)
  -> CGraphBuilderT e s t n m ()
putShapePropagator c p = modPropagators c
    $ Just . (\cp -> cp{shapePropagators=p `ins` shapePropagators cp}) . fromMaybe def
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
  qnfPropagators=HM.alter f c $ qnfPropagators clustConf}
getPropagators :: (Hashables2 e s, MonadReader (ClusterConfig e s t n) m) =>
                 AnyCluster e s t n
               -> m (Maybe (ClustPropagators e s t n))
getPropagators c = asks (HM.lookup c . qnfPropagators)

getShapePropagators :: (Hashables2 e s, MonadReader (ClusterConfig e s t n) m) =>
                     AnyCluster e s t n
                   -> m [(ACPropagator (QueryShape e s) e s t n,
                         [(ShapeSym e s, ShapeSym e s)])]
getShapePropagators c = maybe [] shapePropagators <$> getPropagators c

getNodeShape :: MonadReader (ClusterConfig e s t n) m =>
              NodeRef n -> m (Defaulting (QueryShape e s))
getNodeShape nref = asks (fromMaybe empty . refLU nref . qnfNodeShapes)

modNodeShape :: MonadState (ClusterConfig e s t n) m =>
              NodeRef n
            -> Endo (Defaulting (QueryShape e s))
            -> m ()
modNodeShape nref f = modify $ \clustConf -> clustConf {
  qnfNodeShapes=refAlter
    (Just . f . fromMaybe empty)
    nref
    $ qnfNodeShapes clustConf}
delNodeShape :: Monad m => NodeRef n -> CGraphBuilderT e s t n m ()
delNodeShape nref = modify $ \clustConf -> clustConf {
  qnfNodeShapes=refAdjust demoteDefaulting nref $ qnfNodeShapes clustConf}

-- | Fill the noderefs with shapes.
getShapeCluster :: forall e s t n m .
                 (MonadReader (ClusterConfig e s t n) m, Hashables2 e s) =>
                 AnyCluster e s t n
               -> m (ShapeCluster NodeRef e s t n)
               -- -> CGraphBuilderT e s t n m (ShapeCluster NodeRef e s t n)
getShapeCluster = fmap dropId
                 . bitraverse (withComp $ const $ return empty)
                 (withComp getNodeShape)
                 . putId
  where
    withComp :: (NodeRef a -> m x)
             -> NodeRef a
             -> m (WMetaD x NodeRef a)
    withComp f = fmap WMetaD . (\t -> (,t) <$> f t)
    putId :: AnyCluster e s t n
          -> AnyCluster' (ShapeSym e s) Identity (NodeRef t) (NodeRef n)
    putId = putIdentity
    dropId :: AnyCluster' (ShapeSym e s) Identity
             (WMetaD (Defaulting (QueryShape e s)) NodeRef t)
             (WMetaD (Defaulting (QueryShape e s)) NodeRef n)
           -> ShapeCluster NodeRef e s t n
    dropId = dropIdentity

putShapeCluster
  :: forall e s t n m .
  (MonadState (ClusterConfig e s t n) m,Hashables2 e s)
  => ShapeCluster NodeRef e s t n
  -> m ()
putShapeCluster = void . traverse (uncurry go . unMetaD) . putId
  where
    go :: Defaulting (QueryShape e s) -> NodeRef n -> m ()
    go p ref = ifDefaultingEmpty p (return ()) $ modNodeShape ref $ const p
    putId :: ShapeCluster NodeRef e s t n
          -> AnyCluster'
            (ShapeSym e s)
            Identity
            (WMetaD (Defaulting (QueryShape e s)) NodeRef t)
            (WMetaD (Defaulting (QueryShape e s)) NodeRef n)
    putId = putIdentity

-- | Get a propagator or throw an error if the propagator
getValidClustPropagator
  :: (MonadReader (ClusterConfig e s t n) m
     ,MonadError err m
     ,AShowError e s err
     ,Hashables2 e s
     ,HasCallStack)
  => AnyCluster e s t n
  -> m
    ((ACPropagator (QueryShape e s) e s t n,[(ShapeSym e s,ShapeSym e s)])
    ,ShapeCluster NodeRef e s t n)
getValidClustPropagator clust = do
  propsAssoc <- getShapePropagators clust
  curShapeCluster <- getShapeCluster clust
  -- `assc` is empty..
  let shapeClusters =
        [(prop curShapeCluster,(prop,assc)) | (prop,assc) <- propsAssoc]
  case find (isRight . fst) shapeClusters of
    Just (Right newShapeClust,propAssc) -> return (propAssc,newShapeClust)
    Nothing -> throwAStr
      $ "Expected at least one good shape cluster (some errors are ok): "
      ++ ashow
        (clust,second (first $ const (Sym "<the prop>")) <$> shapeClusters)
    _ -> undefined

triggerClustPropagator :: (MonadState (ClusterConfig e s t n) m,
                          MonadError err m, AShowError e s err,
                          Hashables2 e s) =>
                          AnyCluster e s t n -> m ()
triggerClustPropagator clust = do
  (_,newShapeClust) <- dropReader get $ getValidClustPropagator clust
  putShapeCluster newShapeClust

getNodeShapeFull
  :: MonadReader (ClusterConfig e s t n) m
  => NodeRef n
  -> m (Maybe (QueryShape e s))
getNodeShapeFull r = asks $ getDefaultingFull <=< refLU r . qnfNodeShapes

-- | Assume a consistent view of clusters. Find a trigger that will
-- return a query plen.
--
-- XXX: this fails when called on the bottom
forceQueryShape :: forall e s t n m err .
                 (MonadState (ClusterConfig e s t n) m,
                  MonadError err m, AShowError e s err,
                  Hashables2 e s) =>
                 NodeRef n -> m (Maybe (QueryShape e s))
forceQueryShape n = runMaybeT $ (`evalStateT` mempty) $ go n
  where
    go :: NodeRef n -> StateT (NodeSet n) (MaybeT m) (QueryShape e s)
    go ref = unlessDone $ do
      trail <- get
      guard $ not $ ref `nsMember` trail
      modify (nsInsert ref)
      -- Here we actually need `eitherl`...
      clusts <- lift2
        $ filter (elem ref . clusterOutputs) <$> lookupClustersN ref
      oneOfM clusts $ \c -> do
        case partition (elem ref) [clusterInputs c,clusterOutputs c] of
          ([_siblings],[deps]) -> do
            guard $ not $ any (`nsMember` trail) deps
            mapM_ go deps
            lift2 $ triggerClustPropagator c
            unlessDone $ throwAStr "We made the deps "
          _ -> mzero
      where
        oneOfM [] _  = mzero
        oneOfM cs fm = foldr1Unsafe (<|>) $ fm <$> cs
        unlessDone :: StateT (NodeSet n) (MaybeT m) (QueryShape e s)
                   -> StateT (NodeSet n) (MaybeT m) (QueryShape e s)
        unlessDone m = dropReader (lift2 get) (getNodeShapeFull ref) >>= \case
          Just x  -> return x
          Nothing -> m
