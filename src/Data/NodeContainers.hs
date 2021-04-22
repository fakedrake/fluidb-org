{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Data.NodeContainers
  ( pattern N
  , NodeRef(..)
  , NodeSet(..)
  , RefMap(..)
  , refRestrictKeys
  , refIsSubmapOfBy
  , refIntersectionWithKey
  , refSplitLookup
  , refSplit
  , refMapMaybe
  , refLU
  , refKeys
  , refMember
  , refMapKeys
  , refMapKeysMonotonic
  , refUnion
  , refMapElems
  , refIntersection
  , refInsert
  , refFromNodeSet
  , refAdjust
  , refMapWithKey
  , refDelete
  , refAssocs
  , refFromAssocs
  , refTraverseWithKey
  , refMergeWithKey
  , fromRefAssocs
  , refNull
  , refMergeWithKeyA
  , refUnionWithKey
  , refUnionWithKeyA
  , refAlter
  , nsNull
  , nsMap
  , nsInsert
  , nsDifference
  , nsDelete
  , nsFindDeleteMax
  , nsMember
  , toNodeList
  , toAscNodeList
  , fromNodeList
  , nsSingleton
  , traverseRefMap
  , refToNodeSet
  , nsDisjoint
  , nsIsSubsetOf
  , nsFold
  ) where

import           Control.Arrow            (first, (***))
import           Control.Monad.Identity
import           Data.Char
import           Data.Coerce
import           Data.Function
import           Data.IntMap              (IntMap, Key)
import qualified Data.IntMap              as IM
import qualified Data.IntMap.Merge.Strict as IMM
import qualified Data.IntSet              as IS
import           Data.List
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Function
import           Data.Utils.Hashable
import           GHC.Generics             (Generic)
import           Text.Printf

-- # NODESTRUCT
newtype NodeRef a = NodeRef {runNodeRef :: Key}
  deriving (Eq,Ord,Generic,Functor)
pattern N :: Int -> NodeRef n
pattern N i = NodeRef i

instance AShow (NodeRef n) where
  ashow' (NodeRef r) = sexp "N" [ashow' r]
instance ARead (NodeRef n) where
  aread' = \case
    Sub [Sym "N", r] -> N <$> aread' r
    Sub [Sym "NodeRef", r] -> N <$> aread' r
    _ -> Nothing
instance Show (NodeRef n) where
  show (NodeRef n) = "<" ++ show n ++ ">"
instance Read (NodeRef a) where
  readsPrec _ x = if
    | "NodeRef " `isPrefixOf` norm' x -> go 8
    | "N " `isPrefixOf` norm' x -> go 2
    | otherwise -> []
    where
      norm' = dropWhile (\a -> isSpace a || a == '(')
      go prefSize = first NodeRef <$> reads (drop prefSize x)

instance Integral (NodeRef a) where
  quotRem (NodeRef n) (NodeRef n') =
    let (x,y) = quotRem n n'
    in (NodeRef x,NodeRef y)
  toInteger (NodeRef n) = toInteger n

instance Real (NodeRef a) where
  toRational (NodeRef n) = toRational n

instance Num (NodeRef a) where
  fromInteger = NodeRef . fromInteger
  (+) = NodeRef ... (+) `on` runNodeRef
  (*) = NodeRef ... (*) `on` runNodeRef
  abs = NodeRef . abs . runNodeRef
  negate = NodeRef . negate . runNodeRef
  signum = NodeRef . signum . runNodeRef

instance Enum (NodeRef a) where
  toEnum = NodeRef
  fromEnum = runNodeRef

newtype RefMap n v = RefMap { runRefMap :: IntMap v }
  deriving (Eq, Read, Show, Functor, Foldable, Traversable, Generic)
instance AShow b => AShow (RefMap a b)
instance ARead b => ARead (RefMap a b)
instance Default (RefMap a b)
instance Semigroup (RefMap a b) where (<>) = RefMap ... mappend `on` runRefMap
instance Monoid (RefMap a b) where mempty = RefMap mempty

instance Hashable a => Hashable (RefMap n a)
instance Hashable (NodeRef a)
instance Hashable a => Hashable (IM.IntMap a) where
  hashWithSalt salt = hashWithSalt salt . IM.toList


refLU :: NodeRef from -> RefMap n v -> Maybe v
refLU (NodeRef n) = IM.lookup n . runRefMap
{-# INLINE refLU #-}
refKeys :: RefMap n v -> [NodeRef n]
refKeys = fmap NodeRef . IM.keys . runRefMap
refMember :: NodeRef from -> RefMap n v -> Bool
refMember (NodeRef k) (RefMap m) = k `IM.member` m
refMapKeys :: RefMap n v -> [NodeRef from]
refMapKeys = fmap NodeRef . IM.keys . runRefMap
traverseRefMap :: Applicative m =>
                 (NodeRef n -> v -> m v')
               -> RefMap n v
               -> m (RefMap n v')
traverseRefMap f = fmap RefMap . IM.traverseWithKey (f . NodeRef) . runRefMap

refMergeWithKeyA :: forall f n a b c .
                   Applicative f =>
                  (NodeRef n -> a -> f c)
                -> (NodeRef n -> b -> f c)
                -> (NodeRef n -> a -> b -> f c)
                -> RefMap n a -> RefMap n b -> f (RefMap n c)
refMergeWithKeyA fa fb f l r = fmap RefMap
  $ IMM.mergeA
  (IMM.traverseMissing (fa . NodeRef :: Key -> a -> f c))
  (IMM.traverseMissing (fb . NodeRef  :: Key -> b -> f c))
  (IMM.zipWithAMatched (f . NodeRef :: Key -> a -> b -> f c) :: IMM.WhenMatched f a b c)
  (runRefMap l :: IM.IntMap a)
  (runRefMap r :: IM.IntMap b)

refUnionWithKey :: (NodeRef n -> a -> a -> a) -> RefMap n a -> RefMap n a -> RefMap n a
refUnionWithKey f = runIdentity ... refUnionWithKeyA (\k a b -> Identity $ f k a b)

refUnionWithKeyA :: Applicative f =>
                   (NodeRef n -> a -> a -> f a)
                 -> RefMap n a
                 -> RefMap n a
                 -> f (RefMap n a)
refUnionWithKeyA = refMergeWithKeyA (const pure) (const pure)

refMapKeysMonotonic :: (NodeRef from -> NodeRef from)
                    -> RefMap n v
                    -> RefMap n v
refMapKeysMonotonic f =
  RefMap . IM.mapKeysMonotonic (runNodeRef . f . NodeRef) . runRefMap
refSplitLookup :: NodeRef n -> RefMap n a -> (RefMap n a,Maybe a,RefMap n a)
refSplitLookup (NodeRef k) (RefMap im) = case IM.splitLookup k im of
  (l,x,r) -> (RefMap l,x,RefMap r)
refSplit :: NodeRef n -> RefMap n a -> (RefMap n a,RefMap n a)
refSplit (NodeRef k) (RefMap im) = case IM.split k im of
  (l,r) -> (RefMap l,RefMap r)
refMapMaybe :: (a -> Maybe b) -> RefMap ref a -> RefMap ref b
refMapMaybe f = RefMap . IM.mapMaybe f . runRefMap
refUnion :: RefMap n v -> RefMap n v -> RefMap n v
refUnion (RefMap a) (RefMap b) = RefMap $ IM.union a b
refMapElems :: RefMap n v -> [v]
refMapElems = IM.elems . runRefMap
refIntersection :: RefMap n v -> RefMap n v -> RefMap n v
refIntersection = RefMap ... IM.intersection `on` runRefMap
refIntersectionWithKey :: (NodeRef n -> a -> b -> c)
                       -> RefMap n a -> RefMap n b -> RefMap n c
refIntersectionWithKey f (RefMap l) (RefMap r) =
  RefMap $ IM.intersectionWithKey (f . NodeRef) l r
refInsert :: NodeRef n -> v -> RefMap n v -> RefMap n v
refInsert (NodeRef k) v = RefMap . IM.insert k v . runRefMap
{-# INLINE refInsert #-}
refFromNodeSet :: (NodeRef n -> v) -> NodeSet from -> RefMap n v
refFromNodeSet f = RefMap . IM.fromSet (f . NodeRef) . runNodeSet
refToNodeSet :: RefMap n v -> NodeSet n
refToNodeSet = NodeSet . IM.keysSet . runRefMap
refAdjust :: (v -> v) -> NodeRef n -> RefMap n v -> RefMap n v
refAdjust f (NodeRef k) = RefMap . IM.adjust f k . runRefMap
refAlter :: (Maybe v -> Maybe v) -> NodeRef n -> RefMap n v -> RefMap n v
refAlter f (NodeRef k) = RefMap . IM.alter f k . runRefMap
refMapWithKey :: (NodeRef n -> v -> v') -> RefMap n v -> RefMap n v'
refMapWithKey f = RefMap . IM.mapWithKey (f . NodeRef) . runRefMap
refDelete :: NodeRef from -> RefMap n v -> RefMap n v
refDelete (NodeRef k) = RefMap . IM.delete k . runRefMap
refAssocs :: RefMap n v -> [(NodeRef n,v)]
refAssocs (RefMap m) = [(NodeRef k,v) | (k,v) <- IM.toAscList m]
refRestrictKeys :: RefMap n a -> NodeSet n -> RefMap n a
refRestrictKeys (RefMap r) (NodeSet f) = RefMap $ IM.restrictKeys r f
refIsSubmapOfBy :: (a -> b -> Bool) -> RefMap n a -> RefMap n b -> Bool
refIsSubmapOfBy f (RefMap a) (RefMap b) = IM.isSubmapOfBy f a b
refFromAssocs :: [(NodeRef n,v)] -> RefMap n v
refFromAssocs assoc = RefMap $ IM.fromList [(k,v) | (NodeRef k,v) <- assoc]
refNull :: RefMap n v -> Bool
refNull = IM.null . runRefMap
refTraverseWithKey :: Applicative f =>
                      (NodeRef n -> v -> f v')
                   -> RefMap n v
                   -> f (RefMap n v')
refTraverseWithKey f (RefMap m) = coerce <$> IM.traverseWithKey (f . NodeRef) m
refMergeWithKey :: (NodeRef n -> a -> b -> Maybe c)
                -> (RefMap n a -> RefMap n c)
                -> (RefMap n b -> RefMap n c)
                -> RefMap n a
                -> RefMap n b
                -> RefMap n c
refMergeWithKey f t t' l r = RefMap
  $ IM.mergeWithKey
  (f . NodeRef)
  (runRefMap . t . RefMap)
  (runRefMap . t' . RefMap)
  (runRefMap l)
  (runRefMap r)
fromRefAssocs :: [(NodeRef n,a)] -> RefMap n a
fromRefAssocs = RefMap . IM.fromList . fmap (first fromEnum)

newtype NodeSet n = NodeSet {runNodeSet :: IS.IntSet}
  deriving (Eq, Read, Show, Generic)
instance AShow (NodeSet n) where
  ashow' (NodeSet ns) = sexp "NodeSet" [ashow' ns]
instance ARead (NodeSet n)
instance Default (NodeSet n)
instance Semigroup (NodeSet b) where (<>) = NodeSet ... mappend `on` runNodeSet
instance Monoid (NodeSet b) where mempty = NodeSet mempty
instance Hashable (NodeSet n) where
  hashWithSalt s = IS.foldl' (\h i -> hashWithSalt h i) s . runNodeSet
  {-# INLINE hashWithSalt #-}

nsNull :: NodeSet a -> Bool
nsNull = IS.null . runNodeSet
nsMap :: (NodeRef a -> NodeRef a) -> NodeSet a -> NodeSet a
nsMap f = NodeSet . IS.map (runNodeRef . f . NodeRef) . runNodeSet
nsInsert :: NodeRef a -> NodeSet a -> NodeSet a
nsInsert (NodeRef x) = NodeSet . IS.insert x . runNodeSet
nsDifference :: NodeSet a -> NodeSet a -> NodeSet a
nsDifference (NodeSet x) = NodeSet . IS.difference x . runNodeSet
nsDelete :: NodeRef a -> NodeSet a -> NodeSet a
nsDelete (NodeRef x) = NodeSet . IS.delete x . runNodeSet
nsFindDeleteMax :: NodeSet a -> (NodeRef a, NodeSet a)
nsFindDeleteMax  = (NodeRef *** NodeSet) . IS.deleteFindMax . runNodeSet
nsMember :: NodeRef a -> NodeSet a -> Bool
nsMember (NodeRef x) = IS.member x . runNodeSet
nsIsSubsetOf :: NodeSet a -> NodeSet a -> Bool
nsIsSubsetOf (NodeSet x) (NodeSet y) = IS.isSubsetOf x y
nsDisjoint :: NodeSet a -> NodeSet a -> Bool
nsDisjoint (NodeSet x) (NodeSet y) = IS.disjoint x y
nsSingleton :: NodeRef a -> NodeSet a
nsSingleton (NodeRef x) = NodeSet $ IS.singleton x
toNodeList :: NodeSet a -> [NodeRef a]
toNodeList = fmap NodeRef . IS.toAscList . runNodeSet
toAscNodeList :: NodeSet a -> [NodeRef a]
toAscNodeList = fmap NodeRef . IS.toAscList . runNodeSet
fromNodeList :: [NodeRef a] -> NodeSet a
fromNodeList = NodeSet . IS.fromList . fmap runNodeRef
nsFold :: (NodeRef n -> a -> a) -> NodeSet n -> a -> a
nsFold f ns ini = foldr f ini $ toNodeList ns

-- # /NODESTRUCT
-- data FieldFormat = FieldFormat {
--   fmtWidth     :: Maybe Int,       -- ^ Total width of the field.
--   fmtPrecision :: Maybe Int,   -- ^ Secondary field width specifier.
--   fmtAdjust    :: Maybe FormatAdjustment,  -- ^ Kind of filling or padding
--                                         --   to be done.
--   fmtSign      :: Maybe FormatSign, -- ^ Whether to insist on a
--                                -- plus sign for positive
--                                -- numbers.
--   fmtAlternate :: Bool,        -- ^ Indicates an "alternate
--                                -- format".  See printf(3)
--                                -- for the details, which
--                                -- vary by argument spec.
--   fmtModifiers :: String,      -- ^ Characters that appeared
--                                -- immediately to the left of
--                                -- 'fmtChar' in the format
--                                -- and were accepted by the
--                                -- type's 'parseFormat'.
--                                -- Normally the empty string.
--   fmtChar      :: Char              -- ^ The format character
--                                -- 'printf' was invoked
--                                -- with. 'formatArg' should
--                                -- fail unless this character
--                                -- matches the type. It is
--                                -- normal to handle many
--                                -- different format
--                                -- characters for a single
--                                -- type.
--   }
formatStringC :: Char -> String -> FieldFormatter
formatStringC fmtC x ufmt =
  if fmtChar (vFmt fmtC ufmt) == fmtC
  then (x ++)
  else errorBadFormat $ fmtChar (vFmt fmtC ufmt)

instance PrintfArg (NodeRef a) where
  formatArg x = formatStringC 'n' $ show x
