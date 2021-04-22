module Data.Utils.MTL (dropReader,dropState,dropExcept,eitherToExcept) where

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State

dropReader :: Monad m => m a -> ReaderT a m x -> m x
dropReader askR m = askR >>= runReaderT m
{-# INLINE dropReader #-}

dropState :: Monad m => (m s,s -> m ()) -> StateT s m a -> m a
dropState (getS,putS) m = do
  s <- getS
  (ret,sNew) <- runStateT m s
  putS sNew
  return ret
{-# INLINE dropState #-}

dropExcept f (ExceptT m) = either f return =<< m
dropExcept :: Monad m => (e' -> m x) -> ExceptT e' m x -> m x
{-# INLINE dropExcept #-}

eitherToExcept :: Monad m => Either a b -> ExceptT a m b
eitherToExcept = ExceptT . return
{-# INLINE eitherToExcept #-}
