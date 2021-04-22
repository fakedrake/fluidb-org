{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE TemplateHaskell #-}
module Control.Antisthenis.VarMap (putMech,getUpdMech,ProcMap,VarMap(..)) where

import Control.Monad.Identity
import Data.Utils.Default
import qualified Data.IntMap as IM
import Control.Arrow
import Control.Monad.State
import GHC.Generics
import Control.Antisthenis.Types
import Control.Antisthenis.ATL.Transformers.Mealy
import Control.Antisthenis.Zipper



newtype VarMap a b m =
  VarMap { runVarMap :: IM.IntMap (MealyArrow (Kleisli m) a b) }
  deriving Generic
  deriving (Semigroup,Monoid) via IM.IntMap (MealyArrow (Kleisli m) a b)
instance Default (VarMap a b m)


type family VarMapFamily a :: (* -> *) -> * where
  VarMapFamily (MealyArrow c a b) = VarMap a b
type ProcMap w = VarMapFamily (ArrProc w Identity)

-- The fix state contains normal machines. One may obtain a modmech or
-- the raw machine.
type Updating a = a
getUpdMech
  :: MonadState (VarMap a b m) m
  => b
  -> IM.Key
  -> Updating (MealyArrow (Kleisli m) a b)
getUpdMech err k = MealyArrow $ Kleisli go
  where
    go a = gets (IM.lookup k . runVarMap) >>= \case
      Nothing -> return (MealyArrow $ Kleisli go,err)
      Just (MealyArrow (Kleisli c)) -> do
        (nxt,r) <- c a
        modify $ VarMap . IM.insert k nxt . runVarMap
        return (MealyArrow $ Kleisli go,r)

putMech
  :: MonadState (VarMap a b m) m
  => IM.Key
  -> MealyArrow (Kleisli m) a b
  -> m ()
putMech k mech = modify $ VarMap . IM.insert k mech . runVarMap
