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
import           Data.Bifunctor
import           Data.Bipartite
import           Data.Bitraversable
import           Data.Cluster.ClusterConfig
import           Data.Cluster.Types
import           Data.Cluster.Types.Monad
import           Data.Cluster.Types.Zip
import           Data.CppAst.CppType
import           Data.Either
import           Data.Functor.Identity
import qualified Data.HashMap.Strict                  as HM
import qualified Data.HashSet                         as HS
import           Data.List
import qualified Data.List.NonEmpty                   as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.Optimizations.RemapUnique
import           Data.Query.QuerySchema
import           Data.Query.QuerySchema.SchemaBase
import           Data.Query.QuerySchema.Types
import           Data.Query.QuerySize
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

-- | Scaling
(.*) :: Int -> Double -> Int
i .* d = round $ fromIntegral i * d


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
  QSel _             -> Reversible
  QGroup _ _         -> Irreversible
  QProj QProjNoInv _ -> Irreversible
  QProj _ _          -> Reversible
  QSort _            -> Irreversible
  QLimit _           -> Reversible
  QDrop _            -> Reversible

joinClustPropagator
  :: forall e s t n .
  (HasCallStack,Hashables2 e s)
  => ([(ShapeSym e s,ShapeSym e s)]
     ,[(ShapeSym e s,ShapeSym e s)]
     ,[(ShapeSym e s,ShapeSym e s)])
  -> Prop (Rel (Expr (ShapeSym e s)))
  -> CPropagatorShape JoinClust' e s t n
joinClustPropagator
  (assocOL,assocO,assocOR)
  p
  JoinClust {joinBinCluster = BinClust {..},..} = do
  let il = s binClusterLeftIn
      ir = s binClusterRightIn
      o = s binClusterOut
      oL = s joinClusterLeftAntijoin
      oR = s joinClusterRightAntijoin
  -- Create each output shape but without any sizing until we have
  -- access to the actual. Temporarily use the shape of the input.
  let mkShape :: ([(ShapeSym e s,ShapeSym e s)]
                  -> Defaulting (QueryShape e s)
                  -> Either
                    (AShowStr e s)
                    (Defaulting (QueryShapeNoSize QuerySize e s))) = \m d ->
        defSyncAndDemote <$> traverse (translateShape' id m) d
  oLRTrans <- mkShape (swap <$> assocOR) oR
  oRRTrans <- mkShape (swap <$> assocOL) oL
  iLTransOL <- mkShape assocOL il
  iRTransOR <- mkShape assocOR ir
  -- The output is created in two parts: iLTransO and RTransO.
  iLTransO <- mkShape assocO il
  -- Note that there may be different symbols, the rhs will be the latter ones.
  iRTransO <- mkShape (reverse assocO) ir
  let joinShapesGuard =
        maybe
          (throwAStr
           $ "joinShapes couldn't get a shape:"
           ++ ashow (shapeSymEqs p,qpUnique <$> iLTransO,qpUnique <$> iRTransO))
          return
  (luSide,outShape) <- fmap sequenceA
    $ traverse joinShapesGuard
    $ joinShapes (shapeSymEqs p) <$> iLTransO <*> iRTransO
  let il' = il <> oLRTrans
      ir' = ir <> oRRTrans
      o' = o <> outShape
      oL' = oL <> iLTransOL
      oR' = oR <> iRTransOR
  -- FIX THE SIZES FOR EQUIJOINS!
  -- Note that the size should be the same regardles of
  -- defaultiness. Just fmap into it and insert the most certain
  -- one. The certainty if inferred sizes the certainty of the
  -- outShape.
  (ilSize,olSize) <- updateCardinalities
    (luSide == LeftForeignKey)
    (qpSize <$> il)
    (qpSize <$> oL')
    (qpSize <$> o')
  (irSize,orSize) <- updateCardinalities
    (luSide == RightForeignKey)
    (qpSize <$> ir)
    (qpSize <$> oR')
    (qpSize <$> o')
  let withSize = liftA2 $ modSize . const
  let il'' = withSize ilSize il'
      ir'' = withSize irSize ir'
      o'' = o'
      oL'' = withSize olSize oL'
      oR'' = withSize orSize oR'
  return
    $ updateCHash
      JoinClust
      { joinBinCluster = updateCHash
          BinClust
          { binClusterT = EmptyF
           ,binClusterLeftIn = c il'' binClusterLeftIn
           ,binClusterRightIn = c ir'' binClusterRightIn
           ,binClusterOut = c o'' binClusterOut
           ,binClusterHash = undefined
          }
       ,joinClusterLeftAntijoin = c oL'' joinClusterLeftAntijoin
       ,joinClusterRightAntijoin = c oR'' joinClusterRightAntijoin
       ,joinClusterLeftIntermediate = clrShape joinClusterLeftIntermediate
       ,joinClusterRightIntermediate = clrShape joinClusterRightIntermediate
       ,joinClusterLeftSplit = EmptyF
       ,joinClusterRightSplit = EmptyF
       ,joinClusterHash = undefined
      }
  where
    c :: Defaulting (QueryShape e s)
      -> WMetaD (Defaulting (QueryShape e s)) NodeRef n
      -> WMetaD (Defaulting (QueryShape e s)) NodeRef n
    c v (WMetaD (_,n)) = WMetaD (v,n)
    s (WMetaD (v,_)) = v
    clrShape r = WMetaD (mempty,snd $ unMetaD r)

type HasForeignKey = Bool
-- | Update the cardinalities of input and side output. The Defaulting
-- state of the provided are the correct ones to be
-- outputted. Furthermore the inputs both for in and out side will
-- have the cardinality of the input.
updateCardinalities
  :: MonadAShowErr e s err m
  => HasForeignKey
  -> Defaulting QuerySize -- in
  -> Defaulting QuerySize -- out side
  -> Defaulting QuerySize -- out
  -> m (Defaulting QuerySize,Defaulting QuerySize) -- (new in, new out side)
updateCardinalities True inpD outSideD outD = case getDef outD of
  Nothing -> throwAStr $ "output should not be empty: " ++ ashow outD
  Just out -> return
    (useCardinality out <$> inpD
    ,modCardinality (const 0) <$> outSideD)
updateCardinalities False inpD outSideD _outD =
  return
    (modCardinality (.* 0.7) <$> inpD
    ,modCardinality (.* 0.3) <$> outSideD)

binClustPropagator
  :: Hashables2 e s
  => [(ShapeSym e s,ShapeSym e s)]
  -> BQOp (ShapeSym e s)
  -> CPropagatorShape BinClust' e s t n
binClustPropagator assoc o = mkBinPropagator mkOut mkIn mkIn
  where
    mkIn = G0 Nothing
    mkOut = bopOutput assoc o

unClustPropagator
  :: forall e s t n .
  (HasCallStack,Hashables2 e s)
  => Tup2 [(ShapeSym e s,ShapeSym e s)]
  -> (e -> Maybe CppType)
  -> UQOp (ShapeSym e s)
  -> CPropagatorShape UnClust' e s t n
unClustPropagator symAssocs literalType op =
  mkUnPropagator mkIn mkOutSec mkOutPrim
  where
    mkIn :: G e s (QueryShape e s)
    mkIn = uopInput symAssocs op
    Tup2 mkOutPrim mkOutSec = uopOutputs symAssocs literalType op

-- | The nClustPropagator always creates a full node when triggered.
nClustPropagator
  :: forall e s t n .
  Hashables2 e s
  => QueryShape e s
  -> CPropagatorShape NClust' e s t n
nClustPropagator p (NClust (WMetaD n)) =
  return $ NClust $ WMetaD $ first (const $ mkFull p) n

type SizeOnly x = x
data G e s x
  = G0 (Maybe x)
  | GL (x -> SizeOnly x -> Either (AShowStr e s) x) -- keep the left schema
  | GR (SizeOnly x -> x -> Either (AShowStr e s) x) -- keep the right schema
  | G2 (x -> x -> Either (AShowStr e s) x) -- combine schemata
  | G1 (x -> Either (AShowStr e s) x) -- combine schemata

-- | Combine two defaultings. Trigger the Gfunc and then combine the
-- results according to the semigroup semantics
liftG
  :: forall e s .
  Defaulting (QueryShape e s)
  -> G e s (QueryShape e s)
  -> Defaulting (QueryShape e s)
  -> Defaulting (QueryShape e s)
  -> Either (AShowStr e s) (Defaulting (QueryShape e s))
liftG old gf l0 r0 = (<> old) <$> case gf of
  G0 Nothing   -> return old
  G0 (Just _x) -> error "This doesn't make sence. TODO change the type" -- return $ pure x
  G1 f         -> traverse f l
  GL f         -> sequenceA $ f <$> l <*> r
  GR f         -> sequenceA $ f <$> l <*> r
  G2 f         -> sequenceA $ f <$> l <*> r
  where
    -- We don't want full values to be leaked through the propagators
    l = defSyncAndDemote l0
    r = defSyncAndDemote r0

liftGC
  :: G e s (QueryShape e s)
  -- ^ The group transform
  -> WMetaD (Defaulting (QueryShape e s)) NodeRef n
  -- ^ Left input
  -> WMetaD (Defaulting (QueryShape e s)) NodeRef n
  -- ^ Right input
  -> WMetaD (Defaulting (QueryShape e s)) NodeRef n
  -- ^ Old output
  -> Either (AShowStr e s) (WMetaD (Defaulting (QueryShape e s)) NodeRef n)
liftGC f l r old =
  putMeta old <$> liftG (dropMeta old) f (dropMeta l) (dropMeta r)
  where
    dropMeta (WMetaD (d,_)) = d
    putMeta (WMetaD (_,n)) d = WMetaD (d,n)

mkUnPropagator
  :: forall e s t n .
  Hashables2 e s
  => G e s (QueryShape e s)
  -> G e s (QueryShape e s)
  -> G e s (QueryShape e s)
  -> CPropagatorShape UnClust' e s t n
mkUnPropagator fo1o2_i fio1_o2 fio2_o1 UnClust {..} = do
  unClusterPrimaryOut
    <- liftGC fio2_o1 unClusterIn unClusterSecondaryOut unClusterPrimaryOut
  unClusterSecondaryOut
    <- liftGC fio1_o2 unClusterIn unClusterPrimaryOut unClusterSecondaryOut
  unClusterIn
    <- liftGC fo1o2_i unClusterPrimaryOut unClusterSecondaryOut unClusterIn
  let unClusterT = EmptyF
  return UnClust { .. }

mkBinPropagator
  :: forall e s t n .
  Hashables2 e s
  => G e s (QueryShape e s)
  -> G e s (QueryShape e s)
  -> G e s (QueryShape e s)
  -> CPropagatorShape BinClust' e s t n
mkBinPropagator fiLiR_o fiLo_iR fiRo_iL BinClust {..} = do
  binClusterOut
    <- liftGC fiLiR_o binClusterLeftIn binClusterRightIn binClusterOut
  binClusterLeftIn
    <- liftGC fiRo_iL binClusterRightIn binClusterOut binClusterLeftIn
  binClusterRightIn
    <- liftGC fiLo_iR binClusterLeftIn binClusterOut binClusterRightIn
  let binClusterT = EmptyF
  return BinClust { .. }


-- get the group transform: Prim x Sec -> Inp
uopInput
  :: forall e s .
  (HasCallStack,Hashables2 e s)
  => Tup2 [(ShapeSym e s,ShapeSym e s)]
  -> UQOp (ShapeSym e s)
  -> G e s (QueryShape e s)
uopInput (Tup2 assocPrim assocSec) op = case op of
  QSel _     -> pIn (.* 0.3)
  QGroup _ _ -> G0 Nothing
  QProj _ _  -> G0 Nothing
  QSort _    -> pIn id
  QLimit i   -> pIn (const i)
  QDrop i    -> pIn (`minus` i)
  where
    minus = (-)
    -- Just translate primary output
    pIn modSize = G1 $ \shape -> either
      (const
       $ throwAStr
       $ "[In]Lookup error: "
       ++ ashow (op,swap <$> assocPrim ++ assocSec,shape))
      return
      $ translateShape'
        (modCardinality modSize)
        (swap <$> assocPrim ++ assocSec)
        shape
    swap (a,b) = (b,a)

-- get the group transform: Left x Right -> Out
bopOutput
  :: Hashables2 e s
  => [(ShapeSym e s,ShapeSym e s)]
  -> BQOp (ShapeSym e s)
  -> G e s (QueryShape e s)
bopOutput assoc o = case o of
  QProd -> error
    "Prod should nod be handled by bopOutput, it has it's own kind of cluster"
  QJoin _ -> error
    "Join should nod be handled by bopOutput, it has it's own kind of cluster"
  QUnion -> GL $ flip $ tp mulCard (+) assoc
  QLeftAntijoin _ -> GL $ flip $ tp mulCard (\_ i -> i) assoc
  QRightAntijoin _ -> GR $ tp mulCard (\_ i -> i) (reverse assoc)
  QProjQuery -> error "QProjQuery should have been optimized into a QProj"
  QDistinct -> error "QDistinct should have been optimized into QGroup"
  where
    mulCard = (* 0.5) ... (*)
    tp mkCert mkCard assoc' sizeOnly schema =
      case translateShape' (const newSize) assoc' schema of
        Left _  -> throwAStr $ ashow (void o,length assoc)
        Right x -> return x
      where
        secSize = qpSize sizeOnly
        primSize = qpSize schema
        primTbl = qsTables primSize
        secTbl = qsTables secSize
        primCard = tsRows primTbl
        secCard = tsRows secTbl
        newTbl = primTbl { tsRows = mkCard primCard secCard }
        newSize =
          QuerySize
          { qsTables = newTbl
           ,qsCertainty = mkCert (qsCertainty primSize) (qsCertainty secSize)
          }



-- | Output is Tup2 (Inp x Sec -> Prim) (Inp x Prim -> Sec)
uopOutputs
  :: forall e s .
  (HasCallStack,Hashables2 e s)
  => Tup2 [(ShapeSym e s,ShapeSym e s)]
  -- ^ Tup2 prim sec
  -> (e -> Maybe CppType)
  -> UQOp (ShapeSym e s)
  -> Tup2 (G e s (QueryShape e s))
uopOutputs (Tup2 assocPrim assocSec) literalType op = case op of
  QSel p -> tw (>>= fmap HS.toList . disambEq p . HS.fromList)
  -- For proj and group associations dont mean much
  QGroup p es -> Tup2 (outGrpShape p es) (G0 Nothing)
  QProj inv prj -> Tup2
    (projToShape $ const prj)
    (projToShape $ const [(e,E0 e) | e <- qpiCompl inv])
  QSort _ -> Tup2 (G1 $ pOut id id assocPrim) (G0 Nothing)
  QLimit _ -> tw id
  QDrop _ -> tw id
  where
    tw :: ([[ShapeSym e s]] -> [[ShapeSym e s]])
       -> Tup2 (G e s (QueryShape e s))
    tw modUniq =
      Tup2
        (G1 $ modUniqShape <=< pOut (.* 0.5) (* 0.5) assocPrim)
        (G1 $ pOut (.* 0.5) (* 0.5) assocSec)
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
    pOut :: (Cardinality -> Cardinality)
         -> (Double -> Double)
         -> [(ShapeSym e s,ShapeSym e s)]
         -> QueryShape e s
         -> Either (AShowStr e s) (QueryShape e s)
    pOut modCard modCert assoc shape =
      either
        (const $ throwAStr $ "[Out]Lookup error: " ++ ashow (op,assoc,shape))
        return
      $ translateShape' onSize assoc shape
      where
        onSize qs =
          QuerySize
          { qsTables = (qsTables qs)
              { tsRows = modCard $ tsRows $ qsTables qs }
           ,qsCertainty = modCert $ qsCertainty qs
          }
    outGrpShape p es = G1 $ \inp -> either
      (\e -> throwAStr $ "getQueryShapeGrp failed: " ++ ashow (e,op))
      return
      $ getQueryShapeGrp literalType p es inp
    projToShape :: (QueryShape e s -> [(ShapeSym e s,Expr (ShapeSym e s))])
                -> G e s (QueryShape e s)
    projToShape toPrj = G1 go
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

cPropToACProp
  :: forall c e s t n a ops .
  ((forall f g .
    CanPutIdentity (c f g) (c Identity Identity) f g)
  ,ComposedType c (ShapeSym e s) (WMetaD (Defaulting a) NodeRef)
   ~ WMetaD ops (WMetaD (Defaulting a) NodeRef)
  ,Zip2 (c Identity Identity)
  ,SpecificCluster c)
  => CPropagator a c e s t n
  -> ACPropagator a e s t n
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
  -> ACPropagatorAssoc e s t n
  -> CGraphBuilderT e s t n m ()
putShapePropagator c p =
  modPropagators c
  $ Just
  . (\cp -> cp { shapePropagators = p `ins` shapePropagators cp })
  . fromMaybe def
  where
    -- Note: we insert unique io assocs. The reason is that the assoc
    -- will have the same symbols semantically, only different
    -- provenance. However in case of projection and grouping the
    -- association will be empty and we have no way of telling the
    -- propagators apart.
    ins a@ACPropagatorAssoc {acpaInOutAssoc = []} as = a : as
    ins a@ACPropagatorAssoc {acpaInOutAssoc = kmap} as =
      if kmap `notElem` fmap acpaInOutAssoc as then a : as else as
modPropagators
  :: (Hashables2 e s,Monad m)
  => AnyCluster e s t n
  -> Endo (Maybe (ClustPropagators e s t n))
  -- ^(Propagator,In/Out mapping)
  -> CGraphBuilderT e s t n m ()
modPropagators c f = modify $ \clustConf -> clustConf{
  qnfPropagators=HM.alter f c $ qnfPropagators clustConf}
getPropagators
  :: (Hashables2 e s,MonadReader (ClusterConfig e s t n) m)
  => AnyCluster e s t n
  -> m (Maybe (ClustPropagators e s t n))
getPropagators c = asks (HM.lookup c . qnfPropagators)

getShapePropagators
  :: (Hashables2 e s,MonadReader (ClusterConfig e s t n) m)
  => AnyCluster e s t n
  -> m [ACPropagatorAssoc e s t n]
getShapePropagators c = maybe [] shapePropagators <$> getPropagators c

getNodeShape
  :: MonadReader (ClusterConfig e s t n) m
  => NodeRef n
  -> m (Defaulting (QueryShape e s))
getNodeShape nref = asks (fromMaybe mempty . refLU nref . qnfNodeShapes)

modNodeShape
  :: MonadState (ClusterConfig e s t n) m
  => NodeRef n
  -> Endo (Defaulting (QueryShape e s))
  -> m ()
modNodeShape nref f = do
  modify $ \clustConf -> clustConf
    { qnfNodeShapes = refAlter (Just . f . fromMaybe mempty) nref
        $ qnfNodeShapes clustConf
    }

-- | Fill the noderefs with shapes.
getShapeCluster :: forall e s t n m .
                 (MonadReader (ClusterConfig e s t n) m, Hashables2 e s) =>
                 AnyCluster e s t n
               -> m (ShapeCluster NodeRef e s t n)
getShapeCluster =
  fmap dropId
  . bitraverse (withComp $ const $ return mempty) (withComp getNodeShape)
  . putId
  where
    withComp :: (NodeRef a -> m x) -> NodeRef a -> m (WMetaD x NodeRef a)
    withComp f = fmap WMetaD . (\t -> (,t) <$> f t)
    putId :: AnyCluster e s t n
          -> AnyCluster' (ShapeSym e s) Identity (NodeRef t) (NodeRef n)
    putId = putIdentity
    dropId
      :: AnyCluster'
        (ShapeSym e s)
        Identity
        (WMetaD (Defaulting (QueryShape e s)) NodeRef t)
        (WMetaD (Defaulting (QueryShape e s)) NodeRef n)
      -> ShapeCluster NodeRef e s t n
    dropId = dropIdentity

putShapeCluster
  :: forall e s t n m .
  (MonadState (ClusterConfig e s t n) m,Hashables2 e s,HasCallStack)
  => ShapeCluster NodeRef e s t n
  -> m ()
putShapeCluster = void . traverse (uncurry go . unMetaD) . putId
  where
    go :: Defaulting (QueryShape e s) -> NodeRef n -> m ()
    go p ref = modNodeShape ref (<> p)
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
  -> m (ACPropagatorAssoc e s t n,ShapeCluster NodeRef e s t n)
getValidClustPropagator clust = do
  propsAssoc <- getShapePropagators clust
  curShapeCluster <- getShapeCluster clust
  -- `assc` is empty..
  let shapeClusters =
        [(prop curShapeCluster,acpa)
        | acpa@ACPropagatorAssoc {acpaPropagator = prop} <- propsAssoc]
  case find (isRight . fst) shapeClusters of
    Just (Right newShapeClust,propAssc) -> return (propAssc,newShapeClust)
    Nothing -> throwAStr
      $ "Expected at least one good shape cluster (some errors are ok): "
      ++ ashow
        (clust,second ashowACPA <$> shapeClusters)
    _ -> undefined

triggerClustPropagator
  :: (MonadState (ClusterConfig e s t n) m
     ,MonadError err m
     ,AShowError e s err
     ,HasCallStack
     ,Hashables2 e s)
  => AnyCluster e s t n
  -> m (ShapeCluster NodeRef e s t n)
triggerClustPropagator  clust = do
  (_,newShapeClust) <- dropReader get $ getValidClustPropagator clust
  putShapeCluster newShapeClust
  return newShapeClust

getNodeShapeFull
  :: MonadReader (ClusterConfig e s t n) m
  => NodeRef n
  -> m (Maybe (QueryShape e s))
getNodeShapeFull r = asks $ getDefaultingFull <=< refLU r . qnfNodeShapes

-- | Assume a consistent view of clusters. Find a trigger that will
-- return a query plen.
--
-- XXX: this fails when called on the bottom
forceQueryShape
  :: forall e s t n m err .
  (MonadState (ClusterConfig e s t n) m
  ,MonadError err m
  ,AShowError e s err
  ,Hashables2 e s)
  => NodeRef n
  -> m (Maybe (Defaulting (QueryShape e s)))
forceQueryShape ref0 = (`evalStateT` mempty) $ go ref0
  where
    go :: NodeRef n
       -> StateT (NodeSet n) m (Maybe (Defaulting (QueryShape e s)))
    go ref = do
      trail <- get
      modify (nsInsert ref)
      clusts <- lift
        $ filter (elem ref . clusterOutputs) <$> lookupClustersN ref
      -- Note that the fact that we take3 means it is possible that it
      -- is unstable.
      forM_ (take 3 clusts)
        $ \c -> case partition (elem ref) [clusterInputs c,clusterOutputs c] of
          ([_siblings],[deps]) -> do
            unless (any (`nsMember` trail) deps) $ do
              mapM_ go deps
              void $ lift $ triggerClustPropagator c
          _ -> return ()
      lift $ gets $ refLU ref . qnfNodeShapes
