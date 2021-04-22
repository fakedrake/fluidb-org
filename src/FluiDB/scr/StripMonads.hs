{-# LANGUAGE CPP                    #-}
{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-type-defaults -Wno-unused-top-binds #-}

module Database.FluiDB.Running.StripMonads
  ( mRun
  , ioRun
  , idRun
  , selectGlobalBy
  , ListLike
  , searchForBudget
  ) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.State
import           Control.Monad.Trans.Maybe
import           Data.Maybe

ioRun :: forall e s t n a .
        SqlTypeVars e s t n =>
        GlobalSolveT e s t n IO a
      -> IO a
ioRun = mRun tpchGlobalConf (fail . ("Global error: " ++ ) . ashow)

-- | Use this datatype to push to the end left sides of either.
data MaxOrd a = MOJust a | MOInf deriving (Eq, Show)
instance Ord a => Ord (MaxOrd a) where
  _ <= MOInf = True
  MOInf <= _ = False
  MOJust x <= MOJust y = x <= y


mRun :: forall e s t n a m . Monad m =>
       GlobalConf e s t n
     -> (GlobalError e s t n -> m a)
     -> GlobalSolveT e s t n m a
     -> m a
mRun conf handle s = evalStateT (stripExcept s) conf
  where
    stripExcept :: GlobalSolveT e s t n m a -> StateT (GlobalConf e s t n) m a
    stripExcept = join . fmap handleFailure . runExceptT where
      handleFailure = \case
        Left e -> lift $ handle e
        Right x -> return x

-- | Seach for a budget that makes the query solvable. Assume that the
-- current budget didn't work.
searchForBudget :: forall e s t n m . Monad m =>
                 GlobalSolveT e s t n m Bool
               -> GlobalSolveT e s t n m (Either (Int, Maybe Int) Int)
searchForBudget isSolvableM = budgetBounds 5 Nothing
  where
    budgetBounds :: Int
                 -> Maybe Int
                 -> GlobalSolveT e s t n m (Either (Int, Maybe Int) Int)
    budgetBounds i upper =
      if i <= 0
      then do {b <- getBudget; return $ Left (b, upper)}
      else do
        lower <- getBudget
        let mid = midBudget lower upper
        if mid == lower
          then maybe (nextIter lower mid) (return . Right) upper
          else nextIter lower mid
      where
        nextIter lower mid = do
          setBudget mid
          isSolvable <- sandbox isSolvableM
          if not isSolvable
            then budgetBounds (i-1) upper
            else setBudget lower >> budgetBounds (i-1) (Just mid)
    sandbox m = do {st <- get; ret <- m; put st; return ret}
    midBudget :: Int -> Maybe Int -> Int
    midBudget lower = \case
      Nothing -> lower * 2
      Just upper -> (upper + lower) `div` 2
    setBudget :: Int -> GlobalSolveT e s t n m ()
    setBudget = modify . modBudget . const . Just
    modBudget f conf =
      let oldBudget = budget $ globalGCConfig conf
      in conf {globalGCConfig=(globalGCConfig conf){budget=f oldBudget}}
    getBudget = fromJustErr . budget . globalGCConfig <$> get


idRun :: SqlTypeVars e s t n => CodeBuilderT e s t n [] a -> [a]
idRun = mRun tpchGlobalConf (const $ error "No solution found")
        . runCodeBuild
        . lift

class (Monad l, Monad m) => ListLike m l | l -> m where
  runListLike :: l a -> m [a]
  takeListLike :: Int -> l a -> m [a]
  takeUniqueListLike :: Eq b => (a -> b) -> Int -> l a -> m [a]
  default takeUniqueListLike :: Eq a => (a -> b) -> Int -> l a -> m [a]
  takeUniqueListLike _ i = fmap (take i) . runListLike
  mkListLike :: m [a] -> l a
instance ListLike Identity [] where
  runListLike = return
  takeListLike i = return . take i
  takeUniqueListLike _ _ []     = return []
  takeUniqueListLike _ 0 _      = return []
  takeUniqueListLike _ 1 (x:_)  = return [x]
  takeUniqueListLike f i (x:xs) =
    case runIdentity $ takeUniqueListLike f (i - 1) xs of
      []     -> return [x]
      x':xs' -> return $ if f x == f x' then x':xs' else x:x':xs'
  mkListLike = runIdentity
instance ListLike Identity Identity where
  runListLike = fmap return
  takeListLike i = if i <= 0 then const $ Identity [] else fmap return
  takeUniqueListLike _ i = fmap (take i) . runListLike
  mkListLike = fmap headErr
instance Monad m => ListLike m (MaybeT m) where
  runListLike = fmap maybeToList . runMaybeT
  takeListLike i = if i <= 0
                   then const $ return []
                   else fmap maybeToList . runMaybeT
  takeUniqueListLike _ i = fmap (take i) . runListLike
  mkListLike = MaybeT . fmap listToMaybe
instance Monad m => ListLike m (ListT m) where
  runListLike = runListT
  takeListLike = takeListT . toInteger
  takeUniqueListLike = takeUniqueListT
  mkListLike = mkListT

-- selectListLike $ g . runListLike :: ([(a,s)] -> (b, s))
selectState :: (s -> l (a,s) -> m (b,s))
            -> StateT s l a
            -> StateT s m b
selectState g (StateT f) = StateT $ \s -> g s $ f s
selectExcept :: (l (Either e a) -> m (Either e b))
             -> ExceptT e l a
             -> ExceptT e m b
selectExcept g (ExceptT e) = ExceptT $ g e

-- | From a comparator find the global max.
selectGlobalBy :: forall e s t n m l a . (ListLike m l, Hashables2 e s, Eq a) =>
                 ((a, GlobalConf e s t n) ->
                  (a, GlobalConf e s t n) ->
                 (a, GlobalConf e s t n))
               -> GlobalSolveT e s t n l a
               -> GlobalSolveT e s t n m (Maybe a)
selectGlobalBy isBetter m = selectExcept
  (selectState $ \conf -> fmap (selectList' conf) . takeUniqueListLike fst 1) m
  where
    selectList' :: GlobalConf e s t n
                -> [(Either err a, GlobalConf e s t n)]
                -> (Either err (Maybe a), GlobalConf e s t n)
    selectList' conf x = trace ("Solutions: " ++ show (length x))
                         $ selectListBy isBetter conf x

-- |From a list of possible solutions slect the best one.
selectListBy :: ((a,s) -> (a,s) -> (a,s))
             -> s
             -> [(Either e a, s)]
             -> (Either e (Maybe a), s)
selectListBy best sym = \case
  [] -> (return Nothing, sym)
  (v,s):xs -> case v of
    Left e -> case onlyValues of
      []          -> (Left e, s)
      (v',s'):xs' -> go (v',s') xs'
    Right v' -> go (v',s) onlyValues
    where
      onlyValues = catMaybes
        $ [case v' of {Left _ -> Nothing; Right v'' -> Just (v'',s')}
          | (v',s') <- xs]
      go (defV, defS) = \case
        [] -> (Right $ Just defV, defS)
        (v',s'):rest -> go (best (v',s') (defV, defS)) rest
