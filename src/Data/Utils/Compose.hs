{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveTraversable #-}
module Data.Utils.Compose (Compose(..),interleaveComp) where

import           Control.Applicative
import           Control.Monad
import           Data.Functor.Classes
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           GHC.Generics

newtype Compose f g a = Compose {getCompose :: f (g a)}
  deriving (Generic, Functor, Traversable,Foldable,Eq,Show, Read)
instance (Eq1 f, Eq1 g) => Eq1 (Compose f g) where
  liftEq eq (Compose l) (Compose r) = liftEq (liftEq eq) l r
instance (Applicative f, Applicative g) =>  Applicative (Compose f g) where
  pure = Compose . pure . pure
  Compose fM <*> Compose xM = Compose $ (<*>) (fmap (<*>) fM) xM
instance (Monad f, Monad g, Traversable g) => Monad (Compose f g) where
  return = pure
  Compose xM >>= fM = Compose
    $ fmap join $ sequenceA =<< fmap2 (getCompose . fM) xM
instance Hashable (f (g a)) => Hashable (Compose f g a)
instance (Applicative g,Alternative f) => Alternative (Compose f g) where
  Compose x <|> Compose y = Compose $ x <|> y
  empty = Compose empty
interleaveComp :: (Traversable f,Monad f, Monad m) =>
                 Compose m f (Compose m f a)
               -> Compose m f a
interleaveComp =
  Compose . (fmap join . sequenceA . fmap getCompose =<<) . getCompose
