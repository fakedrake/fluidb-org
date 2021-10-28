{-# LANGUAGE CPP                    #-}
{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveFoldable         #-}
{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE DeriveTraversable      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ImpredicativeTypes     #-}
{-# LANGUAGE InstanceSigs           #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE MultiWayIf             #-}
{-# LANGUAGE OverloadedLists        #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeSynonymInstances   #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-unused-top-binds -Wno-name-shadowing -Wno-orphans #-}

module Data.Cluster.Types.Clusters
  (AnyCluster'(..)
  ,JoinClust'(..)
  ,WMetaD(..)
  ,ashowCluster
  ,destructCluster
  ,updateCHash
  ,primaryTRef
  ,BinClust'(..)
  ,UnClust'(..)
  ,NClust'(..)
  ,AnyCluster
  ,JoinClust
  ,BinClust
  ,UnClust
  ,NClust
  ,Direction(..)
  ,SpecificCluster(..)
  ,CanPutIdentity(..)
  ,clusterInputs
  ,clusterOutputs
  ,clusterInterms
  ,traverseAnyRoles
  ,traverseJoinRoles
  ,traverseBinRoles
  ,traverseUnRoles
  ,primaryNRef
  ,allNodeRefsAnyClust
  ,allNodeRefs
  ,traverseEAnyClust) where

import           Control.Applicative
import           Control.Monad.Identity
import           Control.Monad.Writer
import           Data.Bifoldable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.NodeContainers
import           Data.Proxy
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.Unsafe
import           GHC.Generics

-- | This is a box that holds an annotated value of type `f b`. We use
-- it to insert a value at the edges of a cluster but also annotate
-- them with operators. A cluster may have at it's outputs values of
-- type `WMetaD (BQOp e) (Defaulting (QueryShape e s))`. The important
-- value is `Defaulting (QueryShape e s)` which we use for hashing and
-- equality and other interactions with other clusters but the
-- operator is useful as provenance.
newtype WMetaD a f b = WMetaD { unMetaD :: (a,f b)}
  deriving (Generic,Functor,Traversable,Foldable,Show)
instance (AShow a, AShow (f b)) => AShow (WMetaD a f b)
instance Hashable (f b) => Hashable (WMetaD a f b) where
  hashWithSalt s (WMetaD (_,x)) = hashWithSalt s x
instance Eq (f b) => Eq (WMetaD a f b) where
  WMetaD (_,x) == WMetaD (_,x') = x == x'

-- | Nodes that contribute to a query are part of a cluster. Each
-- cluster is associated with exactly one query. The cluster position
-- of the secondary nodes allows us to derive it from the initial
-- node. Note that the shape of the cluster is useful even if it does
-- not contain NodeRefs. For example we may want to have a mask of
-- which nodes in a cluster are materialized.
type AnyCluster e s = AnyCluster' (ShapeSym e s) NodeRef
data AnyCluster' e f t n =
  JoinClustW (JoinClust' f (ComposedType JoinClust' e f) t n)
  | BinClustW (BinClust' f (ComposedType BinClust' e f) t n)
  | UnClustW (UnClust' f (ComposedType UnClust' e f) t n)
  | NClustW (NClust' f (ComposedType NClust' e f) t n)
  deriving (Eq, Generic)
type JoinClust e s = JoinClust' NodeRef (ComposedType JoinClust' (ShapeSym e s) NodeRef)
type BinClust e s = BinClust' NodeRef (ComposedType BinClust' (ShapeSym e s) NodeRef)
type UnClust e s = UnClust' NodeRef (ComposedType UnClust' (ShapeSym e s) NodeRef)
type NClust e s = NClust' NodeRef (ComposedType NClust' (ShapeSym e s) NodeRef)

instance (Hashables2 (f n) (g t),AShow (g t),AShow (f n))
  => AShow (JoinClust' g f t n) where
  ashow' JoinClust {..} =
    Rec
      "JoinClust"
      [("joinBinCluster",ashow' joinBinCluster)
      ,("joinClusterLeftAntijoin",ashow' joinClusterLeftAntijoin)
      ,("joinClusterRightAntijoin",ashow' joinClusterRightAntijoin)
      ,("joinClusterLeftIntermediate",ashow' joinClusterLeftIntermediate)
      ,("joinClusterRightIntermediate",ashow' joinClusterRightIntermediate)
      ,("joinClusterLeftSplit",ashow' joinClusterLeftSplit)
      ,("joinClusterRightSplit",ashow' joinClusterRightSplit)
      ,("joinClusterHash",ashow' joinClusterHash)]

instance (Hashables2 (f n) (g t),AShow (g t),AShow (f n))
  => AShow (BinClust' g f t n) where
  ashow' BinClust {..} =
    Rec
      "BinClust"
      [("binClusterLeftIn",ashow' binClusterLeftIn)
      ,("binClusterRightIn",ashow' binClusterRightIn)
      ,("binClusterOut",ashow' binClusterOut)
      ,("binClusterT",ashow' binClusterT)
      ,("binClusterHash",ashow' binClusterHash)]

instance (Hashables2 (f n) (g t),AShow (g t),AShow (f n))
  => AShow (UnClust' g f t n) where
  ashow' UnClust {..} =
    Rec
      "UnClust"
      [("unClusterIn",ashow' unClusterIn)
      ,("unClusterPrimaryOut",ashow' unClusterPrimaryOut)
      ,("unClusterSecondaryOut",ashow' unClusterSecondaryOut)
      ,("unClusterT",ashow' unClusterT)
      ,("unClusterHash",ashow' unClusterHash)]

instance (Hashables2 (f n) (g t),AShow (g t),AShow (f n))
  => AShow (NClust' g f t n)
instance (Hashables3 e (f n) (f t),AShow e,AShow (f n),AShow (f t),AShow (f n))
  => AShow (AnyCluster' e f t n)

-- instance (ARead e, ARead (f n), ARead (f n)) => ARead (AnyCluster' e f t n)
-- instance (ARead (g t), ARead (f n)) => ARead (JoinClust' g f t n)
-- instance (ARead (g t), ARead (f n)) => ARead (BinClust' g f t n)
-- instance (ARead (g t), ARead (f n)) => ARead (UnClust' g f t n)
-- instance (ARead (g t), ARead (f n)) => ARead (NClust' g f t n)

newtype NClust' (g :: * -> *) f t n = NClust {nClustNode :: f n}
  deriving (Eq, Show, Read, Functor, Foldable, Traversable, Generic)

data JoinClust' g f t n =
  JoinClust
  { joinBinCluster :: BinClust' g f t n
   ,joinClusterLeftAntijoin :: f n
   ,joinClusterRightAntijoin :: f n
   ,joinClusterLeftIntermediate :: f n
   ,joinClusterRightIntermediate
      :: f n
   ,joinClusterLeftSplit :: g t
   ,joinClusterRightSplit :: g t
   ,joinClusterHash :: Hashables2 (f n) (g t) => Int
  }
instance Hashables2 (f n) (g t) => Eq (JoinClust' g f t n) where
  l == r = hash l == hash r

data BinClust' g f t n =
  BinClust
  { binClusterLeftIn  :: f n
   ,binClusterRightIn :: f n
   ,binClusterOut     :: f n
   ,binClusterT       :: g t
   ,binClusterHash    :: Hashables2 (f n) (g t) => Int
  }
instance Hashables2 (f n) (g t) => Eq (BinClust' g f t n) where
  l == r = hash l == hash r

data UnClust' g f t n =
  UnClust
  { unClusterIn           :: f n
   ,unClusterPrimaryOut   :: f n
   ,unClusterSecondaryOut :: f n
   ,unClusterT            :: g t
   ,unClusterHash         :: Hashables2 (f n) (g t) => Int
  }
instance Hashables2 (f n) (g t) => Eq (UnClust' g f t n) where
  l == r = hash l == hash r

allNodeRefsAnyClust
  :: forall e t n . AnyCluster' e NodeRef t n -> ([NodeRef t],[NodeRef n])
allNodeRefsAnyClust = \case
  JoinClustW x -> allNR x
  BinClustW x  -> allNR x
  UnClustW x   -> allNR x
  NClustW x    -> allNodeRefs0 x
  where
    allNR :: forall c .
          SpecificCluster c
          => c NodeRef (ComposedType c e NodeRef) t n
          -> ([NodeRef t],[NodeRef n])
    allNR = allNodeRefs (Proxy :: Proxy e)

allNodeRefs
  :: forall c t n e .
  SpecificCluster c
  => Proxy e
  -> c NodeRef (ComposedType c e NodeRef) t n
  -> ([NodeRef t],[NodeRef n])
allNodeRefs _ =
  second (fmap $ snd . extractRef (Proxy :: Proxy c) (Proxy :: Proxy e)) .
  allNodeRefs0

primaryTRef :: forall e f t n . AnyCluster' e f t n -> Maybe (f t)
primaryTRef = \case
  JoinClustW x -> Just $ binClusterT $ joinBinCluster x
  BinClustW x  -> Just $ binClusterT x
  UnClustW x   -> Just $ unClusterT x
  NClustW _    -> Nothing

primaryNRef
  :: forall e f t n .
  AnyCluster' e f t n
  -> (Maybe (Either [BQOp e] [UQOp e]),f n)
primaryNRef = \case
  JoinClustW x ->  first (Just . Left) $ unMetaD $ binClusterOut $ joinBinCluster x
  BinClustW x -> first (Just . Left) $ unMetaD $ binClusterOut x
  UnClustW x -> first (Just . Right) $ unMetaD $ unClusterPrimaryOut x
  NClustW x -> (Nothing, nClustNode x)

instance Hashables4
    (f n)
    (f t)
    (ComposedType UnClust' e f n)
    (ComposedType BinClust' e f n)
  => Hashable (AnyCluster' e f t n)
instance Hashables2 (g t) (f n) => Hashable (JoinClust' g f t n) where
  hash = joinClusterHash
  hashWithSalt s = hashWithSalt s . hash
instance Hashables2 (g t) (f n) => Hashable (BinClust' g f t n) where
  hash = binClusterHash
  hashWithSalt s = hashWithSalt s . hash
instance Hashables2 (g t) (f n) => Hashable (UnClust' g f t n) where
  hash = unClusterHash
  hashWithSalt s = hashWithSalt s . hash
instance Hashables2 (g t) (f n) => Hashable (NClust' g f t n)

data Direction
  = ForwardTrigger
  | ReverseTrigger
  deriving (Generic,Show,Read,Eq)
instance Hashable Direction

clusterInputs :: AnyCluster' e f t n -> [f n]
clusterInputs =
  execWriter
  . traverseAnyRoles (\x -> tell [x] >> return x) return return
  . putIdentity
clusterOutputs :: AnyCluster' e f t n -> [f n]
clusterOutputs =
  execWriter
  . traverseAnyRoles return return (\x -> tell [x] >> return x)
  . putIdentity
clusterInterms :: AnyCluster' e f t n -> [f n]
clusterInterms =
  execWriter
  . traverseAnyRoles return (\x -> tell [x] >> return x) return
  . putIdentity
traverseAnyRoles
  :: forall n r t f f' e .
  (Traversable f,Applicative f')
  => (n -> f' r)
  -> (n -> f' r)
  -> (n -> f' r)
  -> AnyCluster' e f t n
  -> f' (AnyCluster' e f t r)
traverseAnyRoles fInput fInterm fOutput = \case
  JoinClustW c -> JoinClustW
    <$> traverseJoinRoles
      (traverse fInput)
      (traverse fInterm)
      (traverse fOutput)
      c
  BinClustW c -> BinClustW
    <$> traverseBinRoles (traverse fInput) (traverse fOutput) c
  UnClustW c -> UnClustW
    <$> traverseUnRoles (traverse fInput) (traverse fOutput) c
  NClustW (NClust x) -> NClustW . NClust <$> traverse fOutput x

traverseUnRoles
  :: Applicative f'
  => (g n -> f' (g r))
  -> (g n -> f' (g r))
  -> UnClust' f g t n
  -> f' (UnClust' f g t r)
traverseUnRoles fInput fOutput UnClust{..} = fmap updateCHash $ UnClust
  <$> fInput unClusterIn
  <*> fOutput unClusterPrimaryOut
  <*> fOutput unClusterSecondaryOut
  <*> pure unClusterT
  <*> pure undefined
traverseBinRoles :: Applicative f' =>
                   (g n -> f' (g r))
                 -> (g n -> f' (g r))
                 -> BinClust' f g t n -> f' (BinClust' f g t r)
traverseBinRoles fInput fOutput BinClust{..} = fmap updateCHash $ BinClust
  <$> fInput binClusterLeftIn
  <*> fInput binClusterRightIn
  <*> fOutput binClusterOut
  <*> pure binClusterT
  <*> pure undefined
traverseJoinRoles :: Applicative f' =>
                    (g n -> f' (g r))
                  -> (g n -> f' (g r))
                  -> (g n -> f' (g r))
                  -> JoinClust' f g t n
                  -> f' (JoinClust' f g t r)
traverseJoinRoles fInput fInterm fOutput JoinClust{..} = fmap updateCHash $ JoinClust
  <$> traverseBinRoles fInput fOutput joinBinCluster
  <*> fOutput joinClusterLeftAntijoin
  <*> fOutput joinClusterRightAntijoin
  <*> fInterm joinClusterLeftIntermediate
  <*> fInterm joinClusterRightIntermediate
  <*> pure joinClusterLeftSplit
  <*> pure joinClusterRightSplit
  <*> pure undefined

instance Traversable f => Bitraversable (AnyCluster' e f) where
  bitraverse f g = \case
    JoinClustW c ->  JoinClustW <$> bitraverse f g c
    BinClustW c  -> BinClustW <$> bitraverse f g c
    UnClustW c   -> UnClustW <$> bitraverse f g c
    NClustW c    -> NClustW <$> bitraverse f g c
instance (Traversable g, Traversable f) => Bitraversable (JoinClust' g f) where
  bitraverse f g JoinClust{..} = fmap updateCHash $ JoinClust
    <$> bitraverse f g joinBinCluster
    <*> traverse g joinClusterLeftAntijoin
    <*> traverse g joinClusterRightAntijoin
    <*> traverse g joinClusterLeftIntermediate
    <*> traverse g joinClusterRightIntermediate
    <*> traverse f joinClusterLeftSplit
    <*> traverse f joinClusterRightSplit
    <*> pure undefined
instance (Traversable g, Traversable f) => Bitraversable (BinClust' g f) where
  bitraverse f g BinClust{..} = fmap updateCHash $ BinClust
    <$> traverse g binClusterLeftIn
    <*> traverse g binClusterRightIn
    <*> traverse g binClusterOut
    <*> traverse f binClusterT
    <*> pure undefined
instance (Traversable g, Traversable f) => Bitraversable (UnClust' g f) where
  bitraverse f g UnClust{..} = fmap updateCHash $ UnClust
    <$> traverse g unClusterIn
    <*> traverse g unClusterPrimaryOut
    <*> traverse g unClusterSecondaryOut
    <*> traverse f unClusterT
    <*> pure undefined
instance (Traversable g, Traversable f) => Bitraversable (NClust' g f) where
  bitraverse _ g NClust{..} = NClust <$> traverse g nClustNode

instance Foldable f => Bifoldable (AnyCluster' g f) where
  bifoldr f g x = \case
    JoinClustW c ->  bifoldr f g x c
    BinClustW c  -> bifoldr f g x c
    UnClustW c   -> bifoldr f g x c
    NClustW c    -> bifoldr f g x c
instance (Foldable g, Foldable f) => Bifoldable (JoinClust' g f) where
  bifoldr :: forall f a b c. Foldable f =>
            (a -> c -> c) -> (b -> c -> c) -> c -> JoinClust' g f a b -> c
  bifoldr f g x JoinClust{..} = bifoldr f g
    (foldr2 g
      (foldr2 f
        x
        ([joinClusterLeftSplit, joinClusterRightSplit] :: [g a]))
      ([joinClusterLeftAntijoin, joinClusterRightAntijoin,
        joinClusterLeftIntermediate, joinClusterRightIntermediate] :: [f b]))
    joinBinCluster
instance (Foldable g, Foldable f) => Bifoldable (BinClust' g f) where
  bifoldr :: forall f a b c. Foldable f =>
            (a -> c -> c) -> (b -> c -> c) -> c -> BinClust' g f a b -> c
  bifoldr f g x BinClust{..} = foldr2 g (foldr f x binClusterT)
    ([binClusterLeftIn,
      binClusterRightIn,
      binClusterOut] :: [f b])
instance (Foldable g, Foldable f) => Bifoldable (UnClust' g f) where
  bifoldr :: forall f a b c. Foldable f =>
            (a -> c -> c) -> (b -> c -> c) -> c -> UnClust' g f a b -> c
  bifoldr f g x UnClust{..} = foldr2 g
    (foldr f x unClusterT)
    ([unClusterIn,
      unClusterPrimaryOut,
      unClusterSecondaryOut] :: [f b])
instance Foldable f => Bifoldable (NClust' g f) where
  bifoldr :: forall f a b c. Foldable f =>
            (a -> c -> c) -> (b -> c -> c) -> c -> NClust' g f a b -> c
  bifoldr _ = foldr

autoFmap :: (Functor f, Bifunctor c', CanPutIdentity c c' g f) =>
           (n -> n') -> c t n -> c t n'
autoFmap f = dropIdentity . bimap id (fmap f) . putIdentity
instance Functor f => Functor (BinClust' g f a) where fmap = autoFmap
instance Functor f => Functor (UnClust' g f a) where fmap = autoFmap
instance Functor f => Functor (JoinClust' g f a) where fmap = autoFmap
instance Functor f => Functor (AnyCluster' g f a) where fmap = autoFmap

instance (Foldable g, Foldable f) => Foldable (BinClust' g f a) where
  foldr = bifoldr (flip const)
instance (Foldable g, Foldable f) => Foldable (UnClust' g f a) where
  foldr = bifoldr (flip const)
instance (Foldable g, Foldable f) => Foldable (JoinClust' g f a) where
  foldr = bifoldr (flip const)
instance Foldable f => Foldable (AnyCluster' e f a) where
  foldr = bifoldr (flip const)

instance (Traversable g, Traversable f) => Traversable (BinClust' g f a) where
  traverse = bitraverse pure
instance (Traversable g, Traversable f) => Traversable (UnClust' g f a) where
  traverse = bitraverse pure
instance (Traversable g, Traversable f) => Traversable (JoinClust' g f a) where
  traverse = bitraverse pure
instance Traversable f => Traversable (AnyCluster' e f a) where
  traverse = bitraverse pure

instance Functor f => Bifunctor (AnyCluster' e f) where
  bimap f g = \case
    JoinClustW x -> JoinClustW $ autoBimap' f g x
    BinClustW x  -> BinClustW $ autoBimap' f g x
    UnClustW x   -> UnClustW $ autoBimap' f g x
    NClustW x    -> NClustW $ autoBimap' f g x
instance (Functor g, Functor f) => Bifunctor (JoinClust' g f) where
  bimap = autoBimap'
instance (Functor g, Functor f) => Bifunctor (UnClust' g f) where
  bimap = autoBimap'
instance (Functor g, Functor f) => Bifunctor (BinClust' g f) where
  bimap = autoBimap'
instance Functor f => Bifunctor (NClust' g f) where
  bimap _  g = NClust . fmap g . nClustNode

autoBimap' ::
     ( Functor f
     , Functor g
     , Bitraversable (c Identity Identity)
     , CanPutIdentity (c g f) (c Identity Identity) g f
     )
  => (t -> t')
  -> (n -> n')
  -> c g f t n
  -> c g f t' n'
autoBimap' f g = dropIdentity . runIdentity . bitraverse (return  . fmap f) (return . fmap g) . putIdentity

-- | Return the unterlying functor of the cluster to identity so that
-- we can access the WMetaD types via bifunctorial interafaces.
class CanPutIdentity c c' g f | c -> c', c -> g, c -> f where
  putIdentity :: c t n -> c' (g t) (f n)
  dropIdentity :: c' (g t) (f n) -> c t n
instance CanPutIdentity (AnyCluster' e f) (AnyCluster' e Identity) f f where
  putIdentity = \case
    JoinClustW x       -> JoinClustW $ putId' x
    BinClustW x        -> BinClustW $ putId' x
    UnClustW x         -> UnClustW $ putId' x
    NClustW (NClust x) -> NClustW $ NClust $ Identity x
  dropIdentity = \case
    JoinClustW x       -> JoinClustW $ dropId' x
    BinClustW x        -> BinClustW $ dropId' x
    UnClustW x         -> UnClustW $ dropId' x
    NClustW (NClust x) -> NClustW $ NClust $ runIdentity x

-- Type tetris here.
putId'
  :: (Bifunctor (c Identity Identity)
     ,CanPutIdentity
        (c Identity (WMetaD a Identity))
        (c Identity Identity)
        Identity
        (WMetaD a Identity)
     ,CanPutIdentity (c g (WMetaD a g)) (c Identity Identity) g (WMetaD a g))
  => c g (WMetaD a g) t n
  -> c Identity (WMetaD a Identity) (g t) (g n)
putId' x =
  dropIdentity
  $ bimap Identity (WMetaD . second Identity . unMetaD)
  $ putIdentity x
dropId'
  :: (Bifunctor (c Identity Identity)
     ,CanPutIdentity
        (c Identity (WMetaD a Identity))
        (c Identity Identity)
        Identity
        (WMetaD a Identity)
     ,CanPutIdentity (c g (WMetaD a g)) (c Identity Identity) g (WMetaD a g))
  => c Identity (WMetaD a Identity) (g t) (g n)
  -> c g (WMetaD a g) t n
dropId' x =
  dropIdentity
  $ bimap runIdentity (WMetaD . second runIdentity . unMetaD)
  $ putIdentity x

instance CanPutIdentity (JoinClust' g f) (JoinClust' Identity Identity) g f where
  putIdentity JoinClust{..} = updateCHash JoinClust {
    joinBinCluster=putIdentity joinBinCluster,
    joinClusterLeftAntijoin=return joinClusterLeftAntijoin,
    joinClusterRightAntijoin=return joinClusterRightAntijoin,
    joinClusterLeftIntermediate=return joinClusterLeftIntermediate,
    joinClusterRightIntermediate=return joinClusterRightIntermediate,
    joinClusterLeftSplit=return joinClusterLeftSplit,
    joinClusterRightSplit=return joinClusterRightSplit,
    joinClusterHash=undefined
    }
  dropIdentity JoinClust{..} = updateCHash JoinClust {
    joinBinCluster=dropIdentity joinBinCluster,
    joinClusterLeftAntijoin=runIdentity joinClusterLeftAntijoin,
    joinClusterRightAntijoin=runIdentity joinClusterRightAntijoin,
    joinClusterLeftIntermediate=runIdentity joinClusterLeftIntermediate,
    joinClusterRightIntermediate=runIdentity joinClusterRightIntermediate,
    joinClusterLeftSplit=runIdentity joinClusterLeftSplit,
    joinClusterRightSplit=runIdentity joinClusterRightSplit,
    joinClusterHash=undefined
    }

instance CanPutIdentity (UnClust' g f) (UnClust' Identity Identity) g f where
  putIdentity UnClust{..} = updateCHash UnClust {
    unClusterIn=return unClusterIn,
    unClusterPrimaryOut=return unClusterPrimaryOut,
    unClusterSecondaryOut=return unClusterSecondaryOut,
    unClusterT=return unClusterT,
    unClusterHash=undefined
    }
  dropIdentity UnClust{..} = updateCHash UnClust {
    unClusterIn=runIdentity unClusterIn,
    unClusterPrimaryOut=runIdentity unClusterPrimaryOut,
    unClusterSecondaryOut=runIdentity unClusterSecondaryOut,
    unClusterT=runIdentity unClusterT,
    unClusterHash=undefined
    }
instance CanPutIdentity (BinClust' g f) (BinClust' Identity Identity) g f where
  putIdentity BinClust{..} = updateCHash BinClust {
    binClusterLeftIn=return binClusterLeftIn,
    binClusterRightIn=return binClusterRightIn,
    binClusterOut=return binClusterOut,
    binClusterT=return binClusterT,
    binClusterHash=undefined
    }
  dropIdentity BinClust{..} = updateCHash BinClust {
    binClusterLeftIn=runIdentity binClusterLeftIn,
    binClusterRightIn=runIdentity binClusterRightIn,
    binClusterOut=runIdentity binClusterOut,
    binClusterT=runIdentity binClusterT,
    binClusterHash=undefined
    }
instance CanPutIdentity (NClust' g f) (NClust' Identity Identity) g f where
  putIdentity = NClust . return . nClustNode
  dropIdentity = NClust . runIdentity . nClustNode

updateCHash :: SpecificCluster c => c f g t n -> c f g t n
updateCHash c = setCHash (hash $ allNodeRefs0 c) c

class SpecificCluster c where
  allNodeRefs0 :: c f g t n -> ([f t],[g n])
  type ComposedType c e (f :: * -> *) :: * -> *

  type ClusterOp c :: * -> *

  toSpecificClust
    :: AnyCluster' e f t n -> Maybe (c f (ComposedType c e f) t n)
  fromSpecificClust :: c f (ComposedType c e f) t n -> AnyCluster' e f t n
  extractRef
    :: Proxy c -> Proxy e -> ComposedType c e f a -> ([ClusterOp c e],f a)
  mkComposedType
    :: Proxy c
    -> Proxy e
    -> ([ClusterOp c e],f a)
    -> Maybe (ComposedType c e f a)
  setCHash :: (Hashables2 (f t) (g n) => Int) -> c f g t n -> c f g t n

instance SpecificCluster JoinClust' where
  allNodeRefs0 JoinClust{..} = bimap
    (++ [joinClusterLeftSplit, joinClusterRightSplit])
    (++ [joinClusterLeftAntijoin,
         joinClusterRightAntijoin,
         joinClusterLeftIntermediate,
         joinClusterRightIntermediate])
    $ allNodeRefs0 joinBinCluster
  type ComposedType JoinClust' e f = WMetaD [BQOp e] f
  type ClusterOp JoinClust' = BQOp
  toSpecificClust = \case
    JoinClustW x -> Just x
    _            -> Nothing
  fromSpecificClust = JoinClustW
  extractRef _ _ = unMetaD
  mkComposedType _ _ = return . WMetaD
  setCHash h jc = jc{joinClusterHash=h}

instance SpecificCluster BinClust' where
  allNodeRefs0 BinClust{..} =
    ([binClusterT], [binClusterLeftIn, binClusterRightIn, binClusterOut])
  type ComposedType BinClust' e f = WMetaD [BQOp e] f
  type ClusterOp BinClust' = BQOp
  toSpecificClust = \case
    BinClustW x -> Just x
    _           -> Nothing
  fromSpecificClust = BinClustW
  extractRef _ _ = unMetaD
  mkComposedType _ _ = return . WMetaD
  setCHash h c = c{binClusterHash=h}

data Void2 :: * -> *
instance Functor Void2 where fmap = undefined
instance Foldable Void2 where foldr = undefined
instance Traversable Void2 where traverse = undefined
instance SpecificCluster UnClust' where
  allNodeRefs0 UnClust{..} =
    ([unClusterT], [unClusterIn, unClusterPrimaryOut, unClusterSecondaryOut])
  type ComposedType UnClust' e f = WMetaD [UQOp e] f
  toSpecificClust = \case
    UnClustW x -> Just x
    _          -> Nothing
  fromSpecificClust = UnClustW
  type ClusterOp UnClust' = UQOp
  extractRef _ _ = unMetaD
  mkComposedType _ _ = return . WMetaD
  setCHash h c = c{unClusterHash=h}

instance SpecificCluster NClust' where
  allNodeRefs0 NClust{..} = ([], [nClustNode])
  type ComposedType NClust' e f = f
  toSpecificClust = \case
    NClustW x -> Just x
    _         -> Nothing
  type ClusterOp NClust' = Void2
  fromSpecificClust = NClustW
  extractRef _ _ r = ([], r)
  mkComposedType _ _ ([], r)  = Just r
  mkComposedType _ _ (_:_, _) = Nothing
  setCHash _ = id

traverseEAnyClust :: Applicative f =>
                    (e -> f e')
                  -> AnyCluster' e NodeRef t n
                  -> f (AnyCluster' e' NodeRef t n)
traverseEAnyClust f c =
  fromJustErr $
  traverseESpecificClust (Proxy :: Proxy JoinClust') f c
  <|> traverseESpecificClust (Proxy :: Proxy BinClust') f c
  <|> traverseESpecificClust (Proxy :: Proxy UnClust') f c
  <|> traverseESpecificClust (Proxy :: Proxy NClust') f c

traverseESpecificClust ::
  forall c e e' f t n .
  (SpecificCluster c, Applicative f,
   Traversable (ClusterOp c),
   CanPutIdentity
    (c NodeRef (ComposedType c e NodeRef))
    (c Identity Identity)
    NodeRef
    (ComposedType c e NodeRef),
   CanPutIdentity
    (c NodeRef (ComposedType c e' NodeRef))
    (c Identity Identity)
    NodeRef
    (ComposedType c e' NodeRef),
   Traversable (c Identity Identity (NodeRef t))) =>
  Proxy c
  -> (e -> f e')
  -> AnyCluster' e NodeRef t n
  -> Maybe (f (AnyCluster' e' NodeRef t n))
traverseESpecificClust _ f =
  fmap (fmap (fromSpecificClust . dropId) . traverse f' . putId)
  . toSpecificClust
  where
    dropId :: c Identity Identity (NodeRef t) (ComposedType c e' NodeRef n)
           -> c NodeRef (ComposedType c e' NodeRef) t n
    dropId = dropIdentity
    putId :: c NodeRef (ComposedType c e NodeRef) t n
          -> c Identity Identity (NodeRef t) (ComposedType c e NodeRef n)
    putId = putIdentity
    f' :: ComposedType c e NodeRef n
       -> f (ComposedType c e' NodeRef n)
    f' =
      fmap ((fromJustErr . mkComposedType pc pe') . swap)
      . (traverse3
           f :: (NodeRef n,[ClusterOp c e]) -> f (NodeRef n,[ClusterOp c e']))
      . swap
      . (extractRef pc pe
           :: ComposedType c e NodeRef n -> ([ClusterOp c e],NodeRef n))
    pe' = Proxy :: Proxy e'
    pe = Proxy :: Proxy e
    pc = Proxy :: Proxy c
    swap (a,b) = (b,a)

destructCluster
  :: AnyCluster' e NodeRef t n -> ([NodeRef n],[NodeRef n],[NodeRef n])
destructCluster c = (clusterInputs c,clusterInterms c,clusterOutputs c)

ashowCluster :: AnyCluster' e NodeRef t n -> SExp
ashowCluster c = sexp name [ashow' $ destructCluster c]
  where
    name = case c of
      JoinClustW _ -> "JoinClustW"
      UnClustW _   -> "UnClustW"
      BinClustW _  -> "BinClustW"
      NClustW _    -> "NClustW"
