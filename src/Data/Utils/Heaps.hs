{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies          #-}
module Data.Utils.Heaps
  (IsHeap(..)
  ,IHeap(..)
  ,CHeap(..)
  ,LHeap(..)
  ,NonNeg(..)
  ,HPrio(..)
  ,chInsert
  ,chMinKey) where

import qualified Data.Heap          as H
import qualified Data.IntMap        as IM
import qualified Data.List.NonEmpty as NEL
import           Data.Utils.AShow
import           Data.Utils.MinElem
import           Data.Utils.Nat
import           GHC.Generics

-- | A stable heap: items with the same key are returned in the order
-- they were inserted. The HeapKey mempty is less than all other
-- heapkeys.
class (forall v . Monoid (h v),Functor h,Ord (HeapKey h))
  => IsHeap h where
  type HeapKey h :: *

  popHeap :: h v -> Maybe ((HeapKey h,v),h v)
  singletonHeap :: HeapKey h -> v -> h v
  maxKeyHeap :: h v -> Maybe (HeapKey h)

data CHeap k a = CHeap { chHeap :: H.Heap (H.Entry k a),chMax :: Maybe k }
  deriving (Generic,Foldable)
instance (AShow k,AShow a) => AShow (CHeap k a)
data LHeap k a = LHeap { lhHeap :: [(k,a)],lhMax :: Maybe k } deriving Functor

chInsert :: Ord k => k -> v  -> CHeap k v -> CHeap k v
chInsert k v CHeap {..} =
  CHeap { chHeap = H.insert (H.Entry k v) chHeap,chMax = case chMax of
    Just m  -> Just $ max m k
    Nothing -> Just k }

chMinKey :: CHeap k v -> Maybe k
chMinKey = fmap (H.priority . fst) . H.uncons . chHeap
instance Ord k =>  Semigroup (LHeap k a) where
  h <> h' =
    LHeap { lhHeap = lhHeap h <> lhHeap h',lhMax = case (lhMax h,lhMax h) of
      (Nothing,Nothing) -> Nothing
      (Just a,Nothing)  -> Just a
      (Nothing,Just a)  -> Just a
      (Just a,Just a')  -> Just $ max a a' }
instance Ord k =>  Monoid (LHeap k a) where
  mempty = LHeap { lhHeap = mempty,lhMax = Nothing }

instance Ord k => IsHeap (LHeap k) where
  type HeapKey (LHeap k) = k
  popHeap LHeap {..} = do
    ((k,v),h) <- viewMinList Nothing [] lhHeap
    let m = if null h then Nothing else lhMax
    return ((k,v),LHeap { lhMax = m,lhHeap = h })
    where
      viewMinList v prev [] = (,prev) <$> v
      viewMinList Nothing prev (x:xs) =
        viewMinList (Just x) prev xs
      viewMinList p@(Just p'@(m,_)) prev (p0@(m0,_):xs) =
        if m0 < m then viewMinList (Just p0) (p' : prev) xs
        else viewMinList p (p0 : prev) xs
  {-# INLINE popHeap #-}
  singletonHeap k v = LHeap { lhMax = Just k,lhHeap = [(k,v)] }
  {-# INLINE singletonHeap #-}
  maxKeyHeap = lhMax
  {-# INLINE maxKeyHeap #-}


instance Ord k => Semigroup (CHeap k a) where
  ch <> ch' =
    CHeap
    { chHeap = chHeap ch <> chHeap ch',chMax = case (chMax ch,chMax ch) of
      (Nothing,Nothing) -> Nothing
      (Just a,Nothing)  -> Just a
      (Nothing,Just a)  -> Just a
      (Just a,Just a')  -> Just $ max a a' }
  {-# INLINE (<>) #-}
instance Ord k =>  Monoid (CHeap k a) where
  mempty = CHeap { chHeap = mempty,chMax = Nothing }

instance Ord k => Functor (CHeap k) where
  fmap f ch =
    CHeap { chHeap = H.mapMonotonic (fmap f) $ chHeap ch,chMax = chMax ch }
  {-# INLINE fmap #-}

instance Ord k => IsHeap (CHeap k) where
  type HeapKey (CHeap k) = k
  popHeap CHeap {..} = do
    (H.Entry k v,h) <- H.viewMin chHeap
    let m = if null h then Nothing else chMax
    return ((k,v),CHeap { chMax = m,chHeap = h })
  {-# INLINE popHeap #-}
  singletonHeap k v =
    CHeap { chMax = Just k,chHeap = H.singleton $ H.Entry k v }
  {-# INLINE singletonHeap #-}
  maxKeyHeap = chMax
  {-# INLINE maxKeyHeap #-}

-- | The heap priority. This is mostly to make the typechecker happy
-- when the ambiguity stemming from the type family HeapKey confuses
-- GHC.
newtype HPrio h = HPrio { getHPrio :: HeapKey h }

newtype IHeap a = IHeap { unIHeap :: IM.IntMap (NEL.NonEmpty a) }
  deriving (Show,Functor,Foldable)

instance Semigroup (IHeap a) where
  IHeap a <> IHeap b = IHeap $ IM.unionWith (<>) a b
  {-# INLINE (<>) #-}
instance Monoid (IHeap a) where
  mempty = IHeap mempty
  {-# INLINE mempty #-}

newtype NonNeg a = NonNeg { getNonNeg :: a }
  deriving (Show,Eq,Ord)
instance (Ord a,Zero a) =>  Zero (NonNeg a) where
  zero = NonNeg zero
  {-# INLINE zero #-}
instance Num a => Semigroup (NonNeg a) where
  NonNeg a <> NonNeg a' = NonNeg $ a + a'

instance (Ord a,Zero a) => MinElem (NonNeg a) where
  minElem = zero
  {-# INLINE minElem #-}

instance IsHeap IHeap where
  type HeapKey IHeap = NonNeg IM.Key
  popHeap (IHeap m) = do
    ((k, h NEL.:| xs0),rest) <- IM.minViewWithKey m
    return ((NonNeg k,h), IHeap $ case NEL.nonEmpty xs0 of
      Nothing -> rest
      Just xs -> IM.insert k xs rest)
  {-# INLINE popHeap #-}
  singletonHeap k v = IHeap $ IM.singleton (getNonNeg k) (pure v)
  {-# INLINE singletonHeap #-}
  maxKeyHeap (IHeap im) = NonNeg . fst . fst <$> IM.maxViewWithKey im
  {-# INLINE maxKeyHeap #-}

-- | A list is a heap where they key is a unit.
instance IsHeap [] where
  type HeapKey [] = ()
  popHeap []     = Nothing
  popHeap (v:vs) = Just (((),v),vs)
  {-# INLINE popHeap #-}
  singletonHeap () = pure
  {-# INLINE singletonHeap #-}
  maxKeyHeap [] = Nothing
  maxKeyHeap _  = Just ()
  {-# INLINE maxKeyHeap #-}
