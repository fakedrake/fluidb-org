{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
module Data.QueryPlan.MetaOpCache
  ( getMetaOpCache
  , putMetaOpCache
  , appendMetaOpCache
  , toMatCache
  , luMatCache
  , delDepMatCache
  ) where

import           Control.Monad.State
import           Data.Foldable
import           Data.NodeContainers
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.Types

appendMetaOpCache :: Monad m => NodeRef n -> (MetaOp t n,Cost) -> PlanT t n m ()
appendMetaOpCache ref ns = modify $ \gcState -> gcState{
  gcCache=(gcCache gcState){
      metaOpCache=refAlter (Just . maybe [ns] (ns:)) ref
        $ metaOpCache (gcCache gcState)
      }}
putMetaOpCache :: Monad m => NodeRef n -> [(MetaOp t n,Cost)] -> PlanT t n m ()
putMetaOpCache ref ns = modify $ \gcState -> gcState{
  gcCache=(gcCache gcState){
      metaOpCache=refInsert ref ns $ metaOpCache (gcCache gcState)}}
{-# INLINE putMetaOpCache #-}
getMetaOpCache :: Monad m => NodeRef n -> PlanT t n m (Maybe [(MetaOp t n,Cost)])
getMetaOpCache ref = refLU ref . metaOpCache . gcCache <$> get
{-# INLINE getMetaOpCache #-}

-- | Delete a dependency node and invalidate the corresponding primary
-- nodes.
delDepMatCache :: Monad m => NodeRef n -> PlanT t n m ()
delDepMatCache dep = do
  modify $ \gcState -> gcState{
    gcCache=(gcCache gcState){
        isMaterializableCache=go $ isMaterializableCache (gcCache gcState)
    }}
  where
    go cache = case refLU dep $ matCacheDeps cache of
      Nothing -> cache
      Just inis -> cache {
        matCacheDeps=refDelete dep $ matCacheDeps cache,
        matCacheVals=foldl (flip refDelete) (matCacheVals cache) inis
        }
luMatCache :: Monad m => NodeRef n -> PlanT t n m (Maybe (Frontiers (MetaOp t n) t n))
luMatCache ref =
  refLU ref . matCacheVals . isMaterializableCache . gcCache <$> get
{-# INLINE luMatCache #-}
toMatCache ::
     forall t n.
     RefMap n (Frontiers (MetaOp t n) t n)
  -> MatCache (MetaOp t n) t n
toMatCache refMap = foldl' go mempty {matCacheVals = refMap} $ refAssocs refMap
  where
    go ::
         MatCache (MetaOp t n) t n
      -> (NodeRef n, Frontiers (MetaOp t n) t n)
      -> MatCache (MetaOp t n) t n
    go cache (ref, Frontiers {frontierStar = (deps, _)}) =
      cache
        { matCacheDeps =
            refUnionWithKey
              (const (++))
              (refFromAssocs $ (, [ref]) <$> toNodeList deps)
              (matCacheDeps cache)
        }
