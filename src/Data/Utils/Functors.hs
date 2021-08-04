module Data.Utils.Functors
  (traverse4
  ,traverse5
  ,traverse3
  ,traverse2
  ,foldr2
  ,foldr3
  ,foldl2
  ,foldl3
  ,fmap4
  ,fmap3
  ,fmap2
  ,toList
  ,toList2
  ,toList3
  ,toList4
  ,toList5
  ,all2
  ,all3
  ,any2
  ,any3
  ,return2
  ,return3
  ,lift2
  ,lift3
  ,lift4
  ,lift5
  ,bitraverse2
  ,cofmap
  ,(<&>),bivoid) where

import           Control.Monad
import           Control.Monad.Trans
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Foldable
import           Data.Utils.Function

traverse4 :: (Applicative f, Traversable t1, Traversable t2, Traversable t3,
             Traversable t4) =>
            (a -> f b) -> t1 (t2 (t3 (t4 a))) -> f (t1 (t2 (t3 (t4 b))))
traverse4 = traverse . traverse3
traverse5 :: (Applicative f, Traversable t1, Traversable t2, Traversable t3,
             Traversable t4, Traversable t5) =>
            (a -> f b) -> t1 (t2 (t3 (t4 (t5 a)))) -> f (t1 (t2 (t3 (t4 (t5 b)))))
traverse5 = traverse . traverse4
traverse3 :: (Applicative f, Traversable t1, Traversable t2, Traversable t3) =>
            (a -> f b) -> t1 (t2 (t3 a)) -> f (t1 (t2 (t3 b)))
traverse3 = traverse . traverse2

traverse2 :: (Applicative f, Traversable t1, Traversable t2) =>
            (a -> f b) -> t1 (t2 a) -> f (t1 (t2 b))
traverse2 = traverse . traverse

bitraverse2 :: (Traversable t, Bitraversable t2, Applicative m) =>
              (a -> m a') -> (b -> m b') -> t (t2 a b) -> m (t (t2 a' b'))
bitraverse2 = traverse ... bitraverse

foldl3 :: (Foldable t1, Foldable t2, Foldable t3) =>
         (b -> a -> b) -> b -> t1 (t2 (t3 a)) -> b
foldl3 = foldl . foldl2

foldl2 :: (Foldable t1, Foldable t2) => (b -> a -> b) -> b -> t1 (t2 a) -> b
foldl2 = foldl . foldl

foldr2 :: (Foldable t1, Foldable t2) => (a -> b -> b) -> b -> t1 (t2 a) -> b
foldr2 = foldr . flip . foldr

foldr3 :: (Foldable t1, Foldable t2, Foldable t3) =>
         (a -> b -> b) -> b -> t1 (t2 (t3 a)) -> b
foldr3 = foldr . flip . foldr2

fmap3 :: (Functor t1, Functor t2, Functor t3) =>
        (a -> b) -> t1 (t2 (t3 a)) -> t1 (t2 (t3 b))
fmap3 = fmap . fmap . fmap
{-# INLINE fmap3 #-}

fmap4 :: (Functor t1, Functor t2, Functor t3, Functor t4) =>
        (a -> b) -> t1 (t2 (t3 (t4 a))) -> t1 (t2 (t3 (t4 b)))
fmap4 = fmap . fmap . fmap . fmap
{-# INLINE fmap4 #-}

fmap2 :: (Functor t1, Functor t2) => (a -> b) -> t1 (t2 a) -> t1 (t2 b)
fmap2 = fmap . fmap
{-# INLINE fmap2 #-}
infixl 4 <&>
(<&>) :: Functor f => f a -> (a -> b) -> f b
(<&>) = flip fmap
{-# INLINE (<&>) #-}

lift2 :: (MonadTrans t1, MonadTrans t, Monad (t m), Monad m) =>
        m a -> t1 (t m) a
lift2 = lift . lift
{-# INLINE lift2 #-}
lift3 :: (Monad (t1 (t m)), Monad (t m), Monad m, MonadTrans t2,
         MonadTrans t, MonadTrans t1) =>
        m a -> t2 (t1 (t m)) a
lift3 = lift . lift . lift
{-# INLINE lift3 #-}
lift4 :: (Monad (t2 (t1 (t m))),Monad (t1 (t m)), Monad (t m), Monad m,
         MonadTrans t3, MonadTrans t2, MonadTrans t, MonadTrans t1) =>
        m a -> t3 (t2 (t1 (t m))) a
lift4 = lift . lift . lift . lift
{-# INLINE lift4 #-}
lift5 :: (Monad (t3 (t2 (t1 (t m)))), Monad (t2 (t1 (t m))), Monad (t1 (t m)),
         Monad (t m), Monad m,
         MonadTrans t4, MonadTrans t3, MonadTrans t2, MonadTrans t1,
         MonadTrans t) =>
        m a -> t4 (t3 (t2 (t1 (t m)))) a
lift5 = lift . lift . lift . lift . lift
{-# INLINE lift5 #-}

return2 :: (Monad m1, Monad m2) => a -> m1 (m2 a)
return2 = return . return

return3 :: (Monad m1, Monad m2, Monad m3) => a -> m1 (m2 (m3 a))
return3 = return . return2
toList4 :: (Foldable t1, Foldable t2, Foldable t3, Foldable t4) =>
          t1 (t2 (t3 (t4 m))) -> [m]
toList4 = toList >=> toList >=> toList >=> toList

toList5 :: (Foldable t1,Foldable t2,Foldable t3,Foldable t4,Foldable t5)
        => t1 (t2 (t3 (t4 (t5 m))))
        -> [m]
toList5 = toList >=> toList >=> toList >=> toList >=> toList


toList3 :: (Foldable t1, Foldable t2, Foldable t3) => t1 (t2 (t3 m)) -> [m]
toList3 = toList >=> toList >=> toList

toList2 :: (Foldable t1, Foldable t2) => t1 (t2 m) -> [m]
toList2 = toList >=> toList

all2 :: (Foldable t1, Foldable t2) => (m -> Bool) -> t1 (t2 m) -> Bool
all2 = all . all

all3 :: (Foldable t1, Foldable t2, Foldable t3) => (m -> Bool) -> t1 (t2 (t3 m)) -> Bool
all3 = all . all . all

any2 :: (Foldable t1, Foldable t2) => (m -> Bool) -> t1 (t2 m) -> Bool
any2 = any . any

any3 :: (Foldable t1, Foldable t2, Foldable t3) => (m -> Bool) -> t1 (t2 (t3 m)) -> Bool
any3 = any . any . any

cofmap :: (forall a . f a -> f' a) -> f x -> f' x
cofmap = id

bivoid :: Bifunctor f => f a b -> f () ()
bivoid = bimap (const ()) (const ())
