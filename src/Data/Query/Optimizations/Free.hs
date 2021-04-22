{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
module Data.Query.Optimizations.Free (runFree,Free) where

data Free f x = Free (f (Free f x)) | Pure (f x)
runFree :: Functor f =>
          (forall a . f (f a) -> f a)
        -> (f x -> f x)
        -> Free f x
        -> f x
runFree interl f = recur where
  recur = \case
    Free x -> interl $ f . recur <$> x
    Pure x -> f x
