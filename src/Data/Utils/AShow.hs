{-# LANGUAGE CPP                  #-}
{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}

module Data.Utils.AShow
  (AShow(..)
  ,aprint
  ,aassert
  ,MonadAShowErr
  ,HasCallStack
  ,SExp(..)
  ,ARead(..)
  ,AShowList
  ,VecType
  ,AShowV
  ,AReadV
  ,mkErrAStr
  ,mkAStr
  ,genericARead'
  ,aread
  ,ashow
  ,sexp
  ,recSexp
  ,genericAShow'
  ,gshow
  ,garead'
  ,AShowStr(..)
  ,AShowError(..)
  ,throwAStr
  ,demoteAStr
  ,ashowCase'
  ,areadCase'
  ,AShow2
  ,AShowV2
  ,ARead2
  ,(<:)) where

import           Data.Utils.AShow.ARead
import           Data.Utils.AShow.AShowStr
import           Data.Utils.AShow.Common
import           Data.Utils.AShow.Print

type AShow2 e s = (AShow e, AShow s,AShowV e, AShowV s)
type AShowV2 e s = (AShowV e, AShowV s)

(<:) :: AShow a => String -> a -> String
msg <: a = msg ++ ": " ++ ashow a
