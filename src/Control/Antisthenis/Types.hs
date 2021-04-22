{-# LANGUAGE DeriveGeneric #-}
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

module Control.Antisthenis.Types
  (Err(..)
  ,LBnd
  ,GBnd
  ,Err
  ,LRes
  ,GRes
  ,InitProc
  ,ItProc
  ,CoitProc) where

import qualified Data.IntSet as IS
import GHC.Generics
import Data.Utils.AShow
import Data.Utils.Default

data Cap b
  = MinimumWork
  | WasFinished
  | ForceResult
  | DoNothing
  | Cap b
  deriving (Show,Functor,Generic)
instance AShow b => AShow (Cap b)
type GBnd v = v
type LBnd v = v
type LRes v = v
type GRes v = v

type InitProc a = a
type ItProc a = a
type CoitProc a = a

data Err
  = ErrMissing Int
  | ErrCycle Int IS.IntSet
  | NoArguments
  deriving (Generic,Show)
instance AShow Err
