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

import Control.Monad.Writer
import Control.Antisthenis.ATL.Class.Functorial
import Control.Antisthenis.ATL.Transformers.Writer
import Control.Monad.Identity
import Data.Utils.Default
import qualified Data.IntMap as IM
import Control.Arrow
import Control.Monad.State
import GHC.Generics
import Control.Antisthenis.Types
import Control.Antisthenis.ATL.Transformers.Mealy

newtype VarMap s a b m =
  VarMap
  { runVarMap :: IM.IntMap (MealyArrow (WriterArrow s (Kleisli m)) a b) }
  deriving Generic
  deriving (Semigroup
           ,Monoid) via IM.IntMap (MealyArrow (WriterArrow s (Kleisli m)) a b)
instance Default (VarMap s a b m)


type family VarMapFamily a :: (* -> *) -> * where
  VarMapFamily (MealyArrow (WriterArrow s c) a b) = VarMap s a b
type ProcMap w = VarMapFamily (ArrProc w Identity)

-- The fix state contains normal machines. One may obtain a modmech or
-- the raw machine.
type Updating a = a
getUpdMech
  :: (Monoid s,MonadState (VarMap s a b m) m)
  => b
  -> IM.Key
  -> Updating (MealyArrow (WriterArrow s (Kleisli m)) a b)
getUpdMech err k = go
  where
    go =
      MealyArrow $ fromKleisli $ \a -> gets (IM.lookup k . runVarMap) >>= \case
        Nothing -> return (go,err)
        Just (MealyArrow c) -> do
          (nxt,r) <- toKleisli c a
          modify $ VarMap . IM.insert k nxt . runVarMap
          return (go,r)

putMech :: (Monoid s,MonadState (VarMap s a b m) m)
        => IM.Key
        -> MealyArrow (WriterArrow s (Kleisli m)) a b
        -> WriterT s m ()
putMech k mech = modify $ VarMap . IM.insert k mech . runVarMap
