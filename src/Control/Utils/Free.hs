{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DeriveDataTypeable    #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE Rank2Types            #-}
{-# LANGUAGE Safe                  #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE UndecidableInstances  #-}

module Control.Utils.Free (FreeF(..),FreeT(..),Free,runFree,hoistFreeT,iterA) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Cont.Class
import           Control.Monad.Error.Class
import qualified Control.Monad.Fail         as Fail
import           Control.Monad.IO.Class
import           Control.Monad.Reader.Class
import           Control.Monad.State.Class
import           Control.Monad.Trans.Class
import           Control.Monad.Writer.Class
import           Data.Bifoldable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Functor.Identity
import           GHC.Generics

-- | The base functor for a free monad.
data FreeF f a b
  = Pure a
  | Free (f b)
  deriving (Eq,Ord,Show,Read,Generic,Generic1)

instance Functor f => Functor (FreeF f a) where
  fmap _ (Pure a)  = Pure a
  fmap f (Free as) = Free (fmap f as)
  {-# INLINE fmap #-}

instance Foldable f => Foldable (FreeF f a) where
  foldMap f (Free as) = foldMap f as
  foldMap _ _         = mempty
  {-# INLINE foldMap #-}

instance Traversable f => Traversable (FreeF f a) where
  traverse _ (Pure a)  = pure (Pure a)
  traverse f (Free as) = Free <$> traverse f as
  {-# INLINE traverse #-}

instance Functor f => Bifunctor (FreeF f) where
  bimap f _ (Pure a)  = Pure (f a)
  bimap _ g (Free as) = Free (fmap g as)
  {-# INLINE bimap #-}

instance Foldable f => Bifoldable (FreeF f) where
  bifoldMap f _ (Pure a)  = f a
  bifoldMap _ g (Free as) = foldMap g as
  {-# INLINE bifoldMap #-}

instance Traversable f => Bitraversable (FreeF f) where
  bitraverse f _ (Pure a)  = Pure <$> f a
  bitraverse _ g (Free as) = Free <$> traverse g as
  {-# INLINE bitraverse #-}

-- | The \"free monad transformer\" for a functor @f@
newtype FreeT f m a = FreeT { runFreeT :: m (FreeF f a (FreeT f m a)) }
  deriving Generic

-- | The \"free monad\" for a functor @f@.
type Free f = FreeT f Identity

-- | Evaluates the first layer out of a free monad value.
runFree :: Free f a -> FreeF f a (Free f a)
runFree = runIdentity . runFreeT
{-# INLINE runFree #-}

instance (Functor f, Monad m) => Functor (FreeT f m) where
  fmap f (FreeT m) = FreeT (fmap f' m) where
    f' (Pure a)  = Pure (f a)
    f' (Free as) = Free (fmap (fmap f) as)

instance (Functor f, Monad m) => Applicative (FreeT f m) where
  pure a = FreeT (return (Pure a))
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}

instance (Functor f,Monad m) => Monad (FreeT f m) where
  return = pure
  {-# INLINE return #-}
  m0 >>= f = go m0 where
    go (FreeT m) = FreeT $ m >>= \case
      Pure a -> runFreeT (f a)
      Free w -> return (Free (go <$> w))
  {-# INLINE (>>=) #-}

instance (Functor f, Fail.MonadFail m) => Fail.MonadFail (FreeT f m) where
  fail e = FreeT (Fail.fail e)

instance MonadTrans (FreeT f) where
  lift = FreeT . fmap Pure
  {-# INLINE lift #-}

instance (Functor f, MonadIO m) => MonadIO (FreeT f m) where
  liftIO = lift . liftIO
  {-# INLINE liftIO #-}

instance (Functor f, MonadReader r m) => MonadReader r (FreeT f m) where
  ask = lift ask
  {-# INLINE ask #-}
  local f = hoistFreeT (local f)
  {-# INLINE local #-}

instance (Functor f,MonadWriter w m) => MonadWriter w (FreeT f m) where
  tell = lift . tell
  {-# INLINE tell #-}
  listen (FreeT m) = FreeT $ concat' <$> listen (fmap listen `fmap` m)
    where
      concat' (Pure x,w) = Pure (x,w)
      concat' (Free y,w) = Free $ fmap (second (w `mappend`)) <$> y
  pass m = FreeT . pass' . runFreeT . hoistFreeT clean $ listen m
    where
      clean = pass . fmap (,const mempty)
      pass' = (>>= g)
      g (Pure ((x,f),w)) = tell (f w) >> return (Pure x)
      g (Free f)         = return . Free . fmap (FreeT . pass' . runFreeT) $ f
  writer w = lift (writer w)
  {-# INLINE writer #-}

instance (Functor f, MonadState s m) => MonadState s (FreeT f m) where
  get = lift get
  {-# INLINE get #-}
  put = lift . put
  {-# INLINE put #-}
  state f = lift (state f)
  {-# INLINE state #-}

instance (Functor f,MonadError e m) => MonadError e (FreeT f m) where
  throwError = lift . throwError
  {-# INLINE throwError #-}
  FreeT m `catchError` f =
    FreeT $ fmap (fmap (`catchError` f)) m `catchError` (runFreeT . f)

instance (Functor f, MonadCont m) => MonadCont (FreeT f m) where
  callCC f = FreeT $ callCC (\k -> runFreeT $ f (lift . k . Pure))

instance (Functor f, MonadPlus m) => Alternative (FreeT f m) where
  empty = FreeT mzero
  FreeT ma <|> FreeT mb = FreeT (mplus ma mb)
  {-# INLINE (<|>) #-}

instance (Functor f, MonadPlus m) => MonadPlus (FreeT f m) where
  mzero = FreeT mzero
  {-# INLINE mzero #-}
  mplus (FreeT ma) (FreeT mb) = FreeT (mplus ma mb)
  {-# INLINE mplus #-}

instance (Foldable m, Foldable f) => Foldable (FreeT f m) where
  foldMap f (FreeT m) = foldMap (bifoldMap f (foldMap f)) m

instance (Monad m,Traversable m,Traversable f) => Traversable (FreeT f m) where
  traverse f (FreeT m) = FreeT <$> traverse (bitraverse f (traverse f)) m

-- | Lift a monad homomorphism from @m@ to @n@ into a monad homomorphism from @'FreeT' f m@ to @'FreeT' f n@
--
-- @'hoistFreeT' :: ('Monad' m, 'Functor' f) => (m ~> n) -> 'FreeT' f m ~> 'FreeT' f n@
hoistFreeT
  :: (Monad m,Functor f)
  => (forall a . m a -> n a)
  -> FreeT f m b
  -> FreeT f n b
hoistFreeT mh = FreeT . mh . fmap (fmap (hoistFreeT mh)) . runFreeT


iterA :: Applicative f => (f (f a) -> f a) -> Free f a -> f a
iterA bind (FreeT (Identity m)) = case m of
  (Pure a) -> pure a
  (Free x) -> bind $ iterA bind <$> x
