{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE KindSignatures    #-}
{-# LANGUAGE LambdaCase        #-}
module Data.Cluster.Types.Query
  ( QueryRef'(..)
  , QueryRef
  , queryRefToQuery
  , getRef
  ) where

import           Data.Bifoldable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Query.Algebra
import           Data.NodeContainers

-- | A query with node references at each layer. A query may
-- correspond to multiple references (although hopefully not too many).
data QueryRef' a n s = QueryRefRec n (a (QueryRef' a n s))
                     | QueryRefPure n (a s)
  deriving (Functor, Traversable, Foldable)
type QueryRef e n = QueryRef' (Query e) (NodeSet n)

instance Functor f => Bifunctor (QueryRef' f) where
  first f = \case
    QueryRefRec a r -> QueryRefRec (f a) $ first f <$> r
    QueryRefPure a r -> QueryRefPure (f a) r
  second = fmap

instance (Applicative t, Foldable t) => Bifoldable (QueryRef' t) where
  bifoldr f g s = \case
    QueryRefRec a r -> f a $ foldr (flip $ bifoldr f g) s r
    QueryRefPure a r -> f a $ foldr g s r

instance (Applicative t, Traversable t) => Bitraversable (QueryRef' t) where
  bitraverse f g = \case
    QueryRefRec a r -> QueryRefRec <$> f a <*> traverse (bitraverse f g)  r
    QueryRefPure a r -> QueryRefPure <$> f a <*> traverse g r

queryRefToQuery :: Monad m => QueryRef' m n s -> m s
queryRefToQuery = \case
  QueryRefRec _ q -> queryRefToQuery =<< q
  QueryRefPure _ q -> q
getRef :: QueryRef' a n s -> n
getRef (QueryRefRec x _)  = x
getRef (QueryRefPure x _) = x
