{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DerivingVia #-}
module Data.Utils.FixState (FixStateT(..)) where

import Control.Monad.Reader
import Control.Monad.State

newtype FixStateT f m a =
  FixStateT { runFixStateT :: f (FixStateT f m) -> m (a,f (FixStateT f m)) }
  deriving (Functor,Applicative,Monad,MonadState (f (FixStateT f m)),MonadFail
           ,MonadReader r) via StateT (f (FixStateT f m)) m
instance MonadTrans (FixStateT f) where
  lift m = FixStateT $ \a -> (,a) <$> m
  {-# INLINE lift #-}
