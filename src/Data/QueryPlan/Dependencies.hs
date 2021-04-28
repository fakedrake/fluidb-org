{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE ViewPatterns          #-}

module Data.QueryPlan.Dependencies
  ( getDependencies
  , isMaterializable
  , getAStar
  , orProd
  ) where

import           Control.Applicative
import           Control.Monad.Except
import           Control.Monad.State
import           Data.Functor.Identity
import qualified Data.HashMap.Strict        as HM
import           Data.List.Extra
import qualified Data.List.NonEmpty         as NEL
import           Data.Maybe
import           Data.NodeContainers
import           Data.QueryPlan.CostTypes
import           Data.QueryPlan.MetaOp
import           Data.QueryPlan.MetaOpCache
import           Data.QueryPlan.Types
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.ListT

-- | AND a sequence of OR expressions. This is lazu
--
-- λ> runListT $ orProd [mkListT $ Identity x
--                      | x <- [[[1,2], [3, 4]],
--d                             [[9,10],[11,12]]]]
-- Identity [[1,2,9,10],[3,4,9,10],[1,2,11,12],[3,4,11,12]]
-- λ> (`runStateT` 0) $ takeListT 3 $ orProd (++) [mkListT $ Identity $ return <$> x | x <- [[1..],[1..],[1..]]]
-- [[1,1,1],[2,1,1],[1,2,1]]
--
-- XXX: do a nub first.
orProd :: Monad m => (a -> a -> a) -> [ListT m a] -> ListT m a
orProd _ [] = mzero
orProd f andLst = foldl1' orProd1 andLst
  where
    orProd1 x y = uncurry f <$> lazyProductListT x y

hoistEvalStateListT :: (Monad m', Monad m) =>
                      s
                    -> (forall x . m (Maybe x) -> s -> m' (Maybe x, s))
                    -> ListT m a
                    -> ListT m' a
hoistEvalStateListT st runSt l = fst <$> hoistRunStateListT runSt st l
{-# INLINE hoistEvalStateListT #-}

isMaterializable :: forall t n m . Monad m => NodeRef n -> PlanT t n m Bool
isMaterializable ref = luMatCache ref >>= \case
  Just _ -> return True
  _ -> isJust <$> headListT (getDependencies ref)

getAStar :: Monad m => NodeRef n -> PlanT t n m Double
getAStar ref = fmap starToDouble  $ luMatCache ref >>= \case
  Just (frontierStar -> (_,star)) -> return star
  Nothing -> headListT (getDependencies ref) >>= \case
    Nothing -> throwPlan $ "Can't find star value: " ++ ashow ref
    Just (_,star) -> return star

data VoidProxy a deriving Functor
instance Applicative VoidProxy where
  (<*>) = undefined -- Unreachable
  pure = undefined -- Unreachable
instance Monad VoidProxy where
  (>>=) = undefined -- Unreachable

getDependencies
  :: forall t n m .
  (HasCallStack,Monad m)
  => NodeRef n
  -> ListT (PlanT t n m) (NodeSet n,StarScore (MetaOp t n) t n)
getDependencies ref0 = do
  stm <- nodeStates . NEL.head . epochs <$> get
  let isMat' ref = case refLU ref stm of
        Just (Initial Mat) -> True
        Just (Concrete _ Mat) -> True
        _ -> False
  cache <- matCacheVals . isMaterializableCache . gcCache <$> get
  hoistEvalStateListT cache storeStateT $ getDependencies' isMat' ref0
  where
    storeStateT
      :: StateT (DCache t n) (PlanT t n m) (Maybe x)
      -> RefMap n (Frontiers (MetaOp t n) t n)
      -> PlanT t n m (Maybe x,RefMap n (Frontiers (MetaOp t n) t n))
    storeStateT m s = do
      ret@(_,newS) <- fmap2 (refMapMaybe id) $ runStateT m $ Just . (,[]) <$> s
      modify $ \gcState -> gcState
        { gcCache = (gcCache gcState)
            { isMaterializableCache = toMatCache $ fst <$> newS }
        }
      return $ fmap2 fst ret
{-# SPECIALISE getDependencies :: NodeRef n
               -> ListT
                 (PlanT t n Identity)
                 (NodeSet n,StarScore (MetaOp t n) t n) #-}

getDependencies'
  :: forall t n m .
  (HasCallStack,Monad m)
  => (NodeRef n -> Bool)
  -> NodeRef n
  -> ListT
    (StateT (DCache t n) (PlanT t n m))
    (NodeSet n,StarScore (MetaOp t n) t n)
getDependencies' isFinalNode ref0 = go [] ref0
  where
    go :: Trail t n
       -> NodeRef n
       -> ListT
         (StateT (DCache t n) (PlanT t n m))
         (NodeSet n,StarScore (MetaOp t n) t n)
    go trail ref0' = withMemoized isFinalNode ref0' trail $ do
      metaOps <- lift2 $ findCostedMetaOps ref0'
      (depset1,mop,mopCost) <- mkListT
        $ return
          [(toNodeList $ metaOpIn mop
           ,mop
           ,mkStarScore mop $ fromIntegral $ costAsInt cost)
          | (mop,cost) <- metaOps]
      let trail' = (mop,ref0') : trail
      when (null depset1) mzero
      (depset,depStarSum) :: (NodeSet n,StarScore (MetaOp t n) t n) <- orProd
        (\(s,d) (s',d') -> (s <> s',d <> d'))
        (go trail' <$> depset1)
      return (depset,depStarSum <> mopCost)

singleFrontiers
  :: (NodeSet n,StarScore (MetaOp t n) t n)
  -> Frontiers (MetaOp t n) t n
singleFrontiers (ns,star) = Frontiers {
  frontierStar=(ns,star), frontierStars=HM.singleton ns star}

insFrontiers
  :: (NodeSet n,StarScore (MetaOp t n) t n)
  -> Frontiers (MetaOp t n) t n
  -> Frontiers (MetaOp t n) t n
insFrontiers (ns,astar) ds = if snd (frontierStar ds) > astar
  then Frontiers {frontierStar=(ns,astar),
                  frontierStars=HM.insert ns astar $ frontierStars ds}
  else ds{frontierStars=HM.insert ns astar $ frontierStars ds}

-- | Top node is the last encountered
type Trail t n = [(MetaOp t n,NodeRef n)]
type DCache t n = RefMap n (Maybe (Frontiers (MetaOp t n) t n,[Trail t n]))
-- Handle marking visited nodes (mask) and memoizing the
-- function.
--
-- XXX: When encountering a node from the same AND path return the
-- original but it's valid only for part of the trail (Same and path:
-- last common node in trail: t). Unioning the star schemata is not
-- enough to get around this, we actually need tha trail thing to
-- avoid orProd from exploding, ie if we find ref n times prod will do
-- (a+b+c+d)^n when (a+b+c+d) would do. ORR have orProd do a nub
-- first.
--
-- XXX: When encountering a node from a different AND path copy
-- the restult. (Same OR path: last common node in trail: n)
--
-- XXX: When encountering a parent die. (Nothing in the)
--
-- XXX: When unencountered just continue
--
-- XXX: The intermediate results need to be consistent on their own.
withMemoized :: forall e t n m . (IsString e,MonadError e m) =>
               (NodeRef n -> Bool)
             -> NodeRef n
             -> Trail t n
             -> ListT (StateT (DCache t n) m)
             (NodeSet n,StarScore (MetaOp t n) t n)
             -> ListT (StateT (DCache t n) m)
             (NodeSet n,StarScore (MetaOp t n) t n)
withMemoized isFin ref trail  m = if isFin ref
  then return (nsSingleton ref,mempty)
  else notFinal
  where
    notFinal = refLU ref <$> get >>= \case
      Just Nothing -> mzero
      Just (Just (frnt,trails)) -> isCached frnt trails
      Nothing -> notCached
    -- We force the set of dependencies because we can't handle cache
    -- a partially evaluated depset. The problem is with resuming.
    -- XXX: we don't mind having fewer deps in the case of
    -- isMaterializable, only in the case of astar.
    notCached = mkListT $ fmap nub $ runListT $ do
      modify $ refInsert ref Nothing
      (rM,accM) <- scanlListT accum (Nothing,Nothing)
        $ fmap Just m <|> return Nothing
      case (rM,accM) of
        -- We now know that ref is materializable but if we encounter
        -- it we have already included all the dependencies. If we
        -- continue to find
        (Just r,_) -> return r
        (Nothing,Just acc) -> modify (refInsert ref $ Just (acc,[trail])) >> mzero
        (Nothing,Nothing) ->  modify (refDelete ref)  >> mzero
    accum (_,Just acc) (Just r) = (Just r,Just $ insFrontiers r acc)
    accum (_,Nothing) (Just r)  = (Just r,Just $ singleFrontiers r)
    accum (_,Just acc) Nothing  = (Nothing,Just acc)
    accum (_,Nothing) Nothing   = (Nothing,Nothing)
    -- | Decide whether it's an and or an or trail
    isCached :: Frontiers (MetaOp t n) t n
             -> [Trail t n]
             -> ListT (StateT (DCache t n) m) (NodeSet n, StarScore (MetaOp t n) t n)
    isCached frnt _trails = mkListT $ return $ HM.toList $ frontierStars frnt
