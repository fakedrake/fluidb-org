{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC  -fno-prof-count-entries -fno-prof-auto -Wno-orphans #-}

module Data.Utils.ListT
  (ListT
  ,NEListT
  ,headNEListT
  ,maybeToListT
  ,headListT
  ,catMaybesListT
  ,filterListT
  ,mkListT
  ,runListT
  ,eitherlListT
  ,takeListT
  ,takeUniqueListT
  ,consListT
  ,alsoFlipListT
  ,alsoListT
  ,lazyProductListT
  ,hoistRunStateListT
  ,foldrListT
  ,unconsListT
  ,mkListT'
  ,foldrListT1
  ,scanlListT) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Except
import           Control.Monad.Morph
import           Control.Monad.Reader.Class
import           Control.Monad.State
import           Control.Monad.Zip
import           Data.Maybe


newtype ListT m a = ListT {unListT :: m (Maybe (a, ListT m a))}
consListT :: Monad m => a -> ListT m a -> ListT m a
consListT x l = ListT $ return $ Just (x, l)
unconsListT :: Monad m => ListT m a -> m (Maybe (a,ListT m a))
unconsListT = unListT

instance Monad m => Semigroup (ListT m a) where
  m1 <> m2 = ListT $ unListT m1 >>= \case
    Nothing -> unListT m2
    Just (h1, s1') -> return $ Just (h1, s1' <> m2)
  {-# INLINE (<>) #-}

instance Monad m => Monoid (ListT m a) where
  mempty = ListT $ return Nothing
  {-# INLINE mempty #-}
  mappend a b = a <> b
  {-# INLINE mappend #-}

instance Functor m => Functor (ListT m) where
  fmap f m = ListT $ fmap (\(a, l) -> (f a, fmap f l)) <$> unListT m
  {-# INLINE fmap #-}

-- We need Monad m for Alternative
instance Monad m => Applicative (ListT m) where
  pure = return
  {-# INLINE pure #-}
  f <*> x = ListT $ go <$> unListT f <*> unListT x
    where
      go Nothing _ = Nothing
      go _ Nothing = Nothing
      go (Just (f',restf)) (Just (x',restx)) =
        Just (f' x',fmap f' restx <|> restf <*> restx)
  {-# INLINE (<*>) #-}

instance (Monad m, Functor m) => Alternative (ListT m) where
  empty = mzero
  {-# INLINE empty #-}
  a <|> b = a <> b
  {-# INLINE (<|>) #-}

instance Monad m => Monad (ListT m) where
  return a = ListT $ return (Just (a, ListT (return Nothing)))
  {-# INLINE return #-}
  s >>= f = ListT $ unListT s >>= \case
    Nothing -> return Nothing
    Just (h, t) -> unListT $ f h <> (t >>= f)
  {-# INLINE (>>=) #-}

instance Monad m => MonadPlus (ListT m) where
  mzero = mempty
  {-# INLINE mzero #-}
  mplus a b = a <> b
  {-# INLINE mplus #-}

instance MonadTrans ListT where
  lift l = ListT $ fmap (\a -> Just (a, mempty)) l
  {-# INLINE lift #-}


maybeToListT :: Monad m => m (Maybe x) -> ListT m x
maybeToListT = mkListT . fmap (\case {Nothing -> []; Just x -> [x]})
headListT :: Monad m => ListT m x -> m (Maybe x)
headListT = fmap listToMaybe . takeListT 1
instance MonadState s m => MonadState s (ListT m) where
  put = lift . put
  {-# INLINE put #-}
  get = lift get
  {-# INLINE get #-}
  state = lift . state
  {-# INLINE state #-}


-- XXX: WARNNG: ONLY WORKS IF f (a >> b) === f a >> f b
-- So NOT $hoist (`evalStateT` x) ...$
--
-- Hoist does work for: ReaderT, ExceptT
instance MFunctor ListT where
  hoist f = ListT . f . (fmap . fmap) (\(a,r) -> (a, hoist f r)) . unListT

instance MonadReader r m => MonadReader r (ListT m) where
  ask = lift ask
  {-# INLINE ask #-}
  local f m = ListT $ local f $ unListT m
  {-# INLINE local #-}

instance MonadError e m => MonadError e (ListT m) where
  throwError = ListT . throwError
  {-# INLINE throwError #-}
  catchError m handler = ListT $ catchError (unListT m) $ unListT . handler
  {-# INLINE catchError #-}

mkListT' :: Monad m => [m a] -> ListT m a
mkListT' []     = mzero
mkListT' (x:xs) = ListT $ x >>= unListT  . (`consListT` mkListT' xs)

mkListT :: Applicative m => m [a] -> ListT m a
mkListT m = ListT $ go <$> m where
  go = \case
    v:vs -> Just (v, mkListT $ pure vs)
    [] -> Nothing

runListT :: Monad m => ListT m a -> m [a]
runListT vM = unListT vM >>= \case
  Nothing -> return []
  Just (v, rest) -> (v:) <$> runListT rest

eitherlListT :: Monad m => ListT m a -> ListT m a -> ListT m a
eitherlListT xM yl = ListT $ unListT xM >>= \case
  Nothing -> case yl of {ListT x' -> x'}
  x@(Just _)  -> return x
alsoListT :: Monad m => ListT m a -> ListT m a -> ListT m a
alsoListT xM yl = ListT $ do
  x <- unListT xM
  case x of
    Nothing        -> return Nothing
    Just (h, rest) -> return $ Just (h, rest <|> yl)
alsoFlipListT :: Monad m => ListT m a -> ListT m a -> ListT m a
alsoFlipListT xM yM = ListT $ unListT xM >>= \case
    Nothing        -> return Nothing
    xMay -> unListT yM >>= \case
      Nothing -> return xMay
      Just (hy, resty) -> return $ Just (hy, resty <|> ListT (return xMay))

instance Monad m => MonadZip (ListT m) where
  mzip m1 m2 = ListT $ do
    l <- unListT m1
    case l of
      Nothing -> return Nothing
      Just (h, rest) -> do
        r <- unListT m2
        return $ case r of
          Nothing          -> Nothing
          Just (h', rest') -> Just ((h, h'), mzip rest rest')

catMaybesListT :: Monad m => ListT m (Maybe a) -> ListT m a
catMaybesListT (ListT m) = ListT $ m >>= \case
  Nothing -> return Nothing
  Just (Nothing,r) -> unListT $ catMaybesListT r
  Just (Just a,r) -> return $ Just (a,catMaybesListT r)

foldrListT :: Monad m => (a -> m b -> m b) -> m b -> ListT m a -> m b
foldrListT f i = recur where
  recur aM = unListT aM >>= \case
    Nothing -> i
    Just (a, aL) -> f a $ recur aL

foldrListT1 :: Monad m => (b -> m b -> m b) -> m b -> ListT m b -> m b
foldrListT1 f def aM = unListT aM >>= \case
    Nothing -> def
    Just (b,bL) -> recur b bL
  where
    recur b bM = unListT bM >>= \case
      Nothing -> return b
      Just (b',bM') -> f b $ recur b' bM'

takeUniqueListT :: forall m a b . (Monad m, Eq b) =>
                  (a -> b) -> Int -> ListT m a -> m [a]
takeUniqueListT f = go Nothing where
  go :: Maybe b -> Int -> ListT m a -> m [a]
  go curM i lst | i <= 0 = return []
                | otherwise =
                  unListT lst >>= \case
                    Nothing        -> return []
                    Just (h, rest) -> case curM of
                      Nothing -> (h:) <$> go (Just $ f h) i rest
                      Just cur -> if cur == f h
                        then go curM i rest
                        else (h:) <$> go (Just $ f h) (i-1) rest


-- takeListT 0 == return []
takeListT :: Monad m => Integer -> ListT m x -> m [x]
takeListT i l = runListT $ fmap snd $ mzip (mkListT $ return [1..i]) l

lazyProductListT :: forall m a b . Monad m => ListT m a -> ListT m b -> ListT m (a, b)
lazyProductListT la lb  = ListT $ go [] [] la lb where
  go :: [a] -> [b] -> ListT m a -> ListT m b -> m (Maybe ((a,b), ListT m (a,b)))
  go prevA prevB aM bM = unListT aM >>= \case
    Nothing -> return Nothing
    Just (a, aL) -> unListT
      $ (<|>) (mkListT $ return [(a,b') | b' <- prevB])
      $ ListT $ unListT bM >>= \case
        Nothing -> return Nothing
        Just (b, bL) -> unListT $ (<|>) (mkListT $ return [(a',b) | a' <- prevA])
          $ ListT
          $ return
          $ Just ((a,b), ListT $ go (prevA ++ [a]) (prevB ++ [b]) aL bL)

-- | Get a sequence of the states.
hoistRunStateListT :: (Monad m, Monad m') =>
                     (forall x . m (Maybe x) -> s -> m' (Maybe x, s))
                   -> s
                   -> ListT m a -> ListT m' (a, s)
hoistRunStateListT runStateT' st aM = ListT $ go <$> runStateT' (unListT aM) st
  where
    recur = hoistRunStateListT runStateT'
    go = \case
      (Nothing, _) -> Nothing
      (Just (a, aL), st') -> Just ((a, st'), recur st' aL)
filterListT :: Monad m => (a -> m Bool) -> ListT m a -> ListT m a
filterListT fM l = ListT $ unListT l >>= \case
  Nothing -> return Nothing
  Just (h,t) ->
    fM h >>= \p -> if p
                  then return $ Just (h, filterListT fM t)
                  else unListT $ filterListT fM t
scanlListT :: Monad m => (b -> a -> b) -> b -> ListT m a -> ListT m b
scanlListT f b (ListT m) = ListT $ m >>= \case
  Just (a,restM) -> let b' = f b a in return $ Just (b',scanlListT f b' restM)
  Nothing -> return Nothing


-- | Non empty ListT.
newtype NEListT m a = NEListT { unNEListT :: m (a,ListT m a)} deriving Functor
nelToListT :: Functor m => NEListT m a -> ListT m a
nelToListT = ListT . fmap Just . unNEListT
{-# INLINE nelToListT #-}
instance Monad m => Applicative (NEListT m) where
  NEListT fM <*> NEListT xM = NEListT $ go <$> fM <*> xM where
    go (f,restf) (x,restx) = (f x,restf <*> restx)
  {-# INLINE (<*>) #-}
  pure x = NEListT $ pure (x,empty)
  {-# INLINE pure #-}
instance Monad m => Monad (NEListT m) where
  NEListT xM >>= f = NEListT $ do
    (x,xs) <- xM
    (y,ys) <- unNEListT $ f x
    return (y,ys <|> (xs >>=  nelToListT . f))
  {-# INLINE (>>=) #-}
  return x = pure x
  {-# INLINE return #-}

instance MonadTrans NEListT where
  lift l = NEListT $ fmap (\a -> (a, mempty)) l
  {-# INLINE lift #-}
instance MonadState s m => MonadState s (NEListT m) where
  put = lift . put
  {-# INLINE put #-}
  get = lift get
  {-# INLINE get #-}
  state = lift . state
  {-# INLINE state #-}
instance MonadReader r m => MonadReader r (NEListT m) where
  ask = lift ask
  {-# INLINE ask #-}
  local f m = NEListT $ local f $ unNEListT m
  {-# INLINE local #-}
instance MonadError e m => MonadError e (NEListT m) where
  throwError = NEListT . throwError
  {-# INLINE throwError #-}
  catchError m handler =
    NEListT $ catchError (unNEListT m) $ unNEListT . handler
  {-# INLINE catchError #-}

instance Monad m => Semigroup (NEListT m a) where
  a <> b = NEListT $ fmap (fmap (<> nelToListT b)) $ unNEListT a
  {-# INLINE (<>) #-}

headNEListT :: Functor m => NEListT m a -> m a
headNEListT (NEListT m) = fmap fst m
