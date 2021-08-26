{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# OPTIONS_GHC -O2 -fno-prof-count-entries -fno-prof-auto #-}

-- |Best first search monad with continuations.

module Data.Utils.HContT
  ( HContT(..)
  , dissolve'
  , eitherlHCont
  , dissolve
  , runHContT'
  , MonadHalt(..)
  , Partitionable
  , cutContT
  ) where

import           Control.Applicative
import           Control.Monad.Cont
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer
import           Data.Bifunctor
import           Data.Either
import           Data.Foldable
import           Data.Group
import           Data.List
import           Data.Utils.Debug

-- | See: Fischer, Sebastian. "Reinventing Haskell Backtracking." GI
--  Jahrestagung 2009 (2009): 2875-2888.
-- https://www.aaai.org/Papers/AAAI/1987/AAAI87-011.pdf


type Partitionable m = (MonadPlus m,Foldable m)
-- | A continuation of steps.
newtype HContT v r m a = HContT {
  unHContT :: (a -> m (Either (v, HContT v r m r) r))
            -> m (Either (v, HContT v r m r) r)
  }
runHContT' :: Monad m => HContT v r m a -> (a -> m r) -> m r
runHContT' c f = unHContT c (fmap Right . f) >>= \case
  Right x     -> return x
  Left (_, r) -> runHContT' r return

instance Functor m => Functor (HContT v r m) where
  fmap f m = HContT $ \ c -> unHContT m (c . f)
  {-# INLINE fmap #-}
instance Functor m => Applicative (HContT v r m) where
  pure x  = HContT ($ x)
  {-# INLINE pure #-}
  f <*> v = HContT $ \c -> unHContT f $ \g -> unHContT v (c . g)
  {-# INLINE (<*>) #-}
  m *> k = m >>= const k
  {-# INLINE (*>) #-}
instance Functor m => Monad (HContT v r m) where
  m >>= k  = HContT $ \ c -> unHContT m (\ x -> unHContT (k x) c)
  {-# INLINE (>>=) #-}
instance MonadTrans (HContT v r) where
  lift m = HContT (m >>=)
  {-# INLINE lift #-}

dissolve :: forall m v r . (Ord v, Foldable m, Applicative m, Semigroup v) =>
           HContT v r m r -> [r]
dissolve = dissolve' $ pure . Right

-- | Dissolve a continuation into a lazy list.
dissolve'
  :: forall m v r a .
  (Ord v,Foldable m,Applicative m,Semigroup v)
  => (a -> m (Either (v,HContT v r m r) r))
  -> HContT v r m a
  -> [r]
dissolve' f = go . dissolve1 f
  where
    go :: ([(v,HContT v r m r)],[r]) -> [r]
    go (cs0,rs) = rs ++ go'
      where
        go' = case cs0 of
          [] -> []
          (v,c):cs -> go
            $ first ((`merge` cs) . fmap (first (v <>)))
            $ dissolve1 (pure . Right) c

eitherlHCont :: forall v r m x . (Ord v,Foldable m,MonadPlus m,Group v) =>
               HContT v r m x -> HContT v r m x -> HContT v r m x
eitherlHCont = recur [] where
  recur :: forall a . [v] -> HContT v r m a -> HContT v r m a -> HContT v r m a
  recur vs a b = HContT go where
    go f = msum $ fmap return
      $ case dissolve1 f a of
          (cs,rs@(_:_)) -> fmap Right rs ++ fmap Left cs
          ([],[])       -> return $ Left (invconcat vs,b')
          (cs,[])       -> Left <$> onLast putB cs
      where
        putB (v,a') = (v,recur (v:vs) a' b')
        b' = HContT $ \f' -> bind2 f' (unHContT b f)
  -- a <> mconcat xs <> invconcat xs <> b = a <> b
  -- => mconcat a <> invconcata  = id
  invconcat :: [v] -> v
  invconcat = invert . mconcat
  onLast f = \case
    []   -> []
    [x]  -> [f x]
    x:xs -> x:onLast f xs

-- XXX: instead of foldable we actually need Ord a => m (Either a b) -> m ([a],[b])
dissolve1
  :: (Ord v,Foldable m)
  => (x -> m (Either (v,HContT v r m r) r))
  -> HContT v r m x
  -> ([(v,HContT v r m r)],[r])
dissolve1 f = first (sortOn fst) . partitionEithers . toList . (`unHContT` f)

merge :: Ord a => [(a, b)] -> [(a, b)] -> [(a, b)]
merge [] x          = x
merge x []          = x
merge (x:xs) (y:ys) = if fst x <= fst y
                      then x:merge xs (y:ys)
                      else y:merge (x:xs) ys

bind2 :: Monad m =>
        (a -> m (Either (v, HContT v r m r) b))
      -> m (Either (v, HContT v r m r) a)
      -> m (Either (v, HContT v r m r) b)
bind2 f mmx = mmx >>= either (return . Left) f

class Monad m => MonadHalt m where
  type HValue m :: *
  halt :: HValue m -> m ()
instance MonadHalt m => MonadHalt (StateT s m) where
  type HValue (StateT s m) = HValue m
  halt = lift . halt
instance MonadHalt m => MonadHalt (ReaderT s m) where
  type HValue (ReaderT s m) = HValue m
  halt = lift . halt
instance (Monoid s, MonadHalt m) => MonadHalt (WriterT s m) where
  type HValue (WriterT s m) = HValue m
  halt = lift . halt
instance MonadHalt m => MonadHalt (ExceptT e m) where
  type HValue (ExceptT e m) = HValue m
  halt = lift . halt
instance (Ord v,Foldable m,MonadPlus m) => MonadHalt (HContT v r m) where
  type HValue (HContT v r m) = v
  halt v = HContT $ \fin -> return $ Left (v, HContT $ \f -> bind2 f (fin ()))
instance Alternative m => Alternative (HContT v r m) where
  a <|> b = HContT $ \c -> unHContT a c <|> unHContT b c
  empty = HContT $ const empty
instance MonadPlus m => MonadPlus (HContT v r m) where
-- | Schedule an HContT internally promoting at each halt the minimum
-- to the outer schedule. When a result is reach ONLY that one is
-- returned.
cutContT :: (Ord v,Foldable m,MonadPlus m,HValue m'~v,MonadHalt m') =>
           m' a -> HContT v a m a -> m' a
cutContT bot = recur where
  recur m = case dissolve1 (pure . Right) m of
    ([],[])       -> bot
    (_,a:_)       -> return a
    ((v,c):cs,[]) -> (halt v >>) $ case dissolve1 (pure . Right) c of
      (_,r:_)  -> return r
      (cs',[]) -> recur $ HContT $ \f ->
        bind2 f $ msum $ pure . Left <$> merge cs' cs

#if 0
-- "cad"
test :: [Char]
test = dissolve
  $ cutContT mzero (h 3 >> ((h 1 >> return 'a') <|> (h 2 >> return 'b')))
  <|> (h 3 >> return 'c') <|> (h 5 >> return 'd')
  where
    h :: Int -> HContT (Sum Int) Char [] ()
    h = halt . Sum
#endif
