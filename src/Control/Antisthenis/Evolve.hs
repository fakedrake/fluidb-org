{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveFunctor #-}

module Control.Antisthenis.Evolve (Ev,runEv,mealyToEv,evToMealy) where

import Control.Arrow
import Control.Antisthenis.ATL.Transformers.Mealy
import Control.Monad.Reader

-- | [Ev] An evolving process that has multiple ways of progressing
-- captured by cmd.
newtype Fix f = Fix { runFix :: f (Fix f) }
newtype EvF m v a = EvF { runEvF :: m (a,v) }
type Ev m v = Fix (EvF m v)
runEv :: Ev m v -> m (Ev m v,v)
runEv = runEvF . runFix
{-# INLINE runEv #-}

mkEv :: m (Ev m v,v) -> Ev m v
mkEv = Fix . EvF
{-# INLINE mkEv #-}

-- Note: these might even be coercible.
mealyToEv :: Monad m => MealyArrow (Kleisli m) a b -> Ev (ReaderT a m) b
mealyToEv (MealyArrow (Kleisli k)) =
  mkEv $ fmap (first mealyToEv) $ ReaderT k
evToMealy :: Monad m => Ev (ReaderT a m) b -> MealyArrow (Kleisli m) a b
evToMealy m =
  MealyArrow $ Kleisli $ runReaderT $ fmap (first evToMealy) $ runEv m
