{-# LANGUAGE ConstraintKinds           #-}
{-# LANGUAGE DeriveFoldable            #-}
{-# LANGUAGE DeriveFunctor             #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE DeriveTraversable         #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE UndecidableInstances      #-}
module Data.Query.Optimizations.Types
  ( MonadOpt
  , OptimizationError(..)
  , TightenConf(..)
  , TightenErr(..)
  , SymEmbedding(..)
  , planSymSymEmbedding
  , mapAStr
  , WrapEq(..)
  , eqSymEmbedding
  , unWrapEq
  ) where

import           Control.Monad.Except
import           Data.QnfQuery.Types
import           Data.Constraint        as DC
import           Data.CppAst.CppType
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

type MonadOpt e s t n m = MonadError (OptimizationError (ExpTypeSym' e) s t n) m

newtype PTrav e = PTrav (Prop (Rel (Expr e)))
                deriving (Functor, Foldable, Traversable)

data OptimizationError e s t n = OptErrorAStr (AShowStr e s) deriving (Generic,Eq)
instance (AShowV e, AShowV s) => AShow (OptimizationError e s t n)
instance AShowError e s (OptimizationError e s t n)
newtype TightenErr e s = TightenErrMsg { unTightenErrMsg :: AShowStr e s}
  deriving (Eq,Generic)
instance (AShowV e, AShowV s) => AShow (TightenErr e s)
instance AShowError e s (TightenErr e s)
data TightenConf e s = TightenConf {
  tightenConfIsLit   :: e -> Bool,
  tightenConfIsInSym :: e -> s -> Bool,
  tightenConfESymEq  :: e -> e -> Bool,
  tightenConfSSymEq  :: s -> s -> Bool
  }
data SymEmbedding e s e' = SymEmbedding {
  embedLit   :: e -> e',
  unEmbed    :: e' -> e,
  embedIsLit :: e' -> Bool,
  symEq      :: e' -> e' -> Bool,
  embedType  :: e' -> Maybe CppType,
  embedInS   :: e' -> s -> Bool
  }

mapAStr :: ((AShowV e,AShowV s) DC.:- (AShowV e',AShowV s'))
        -> AShowStr e' s' -> AShowStr e s
mapAStr sub (AShowStr f) = AShowStr
  $ \d -> f $ DC.mapDict sub d

planSymSymEmbedding :: Hashables2 e s =>
                      (s' -> s)
                    -> (e -> CppType)
                    -> SymEmbedding e s' (PlanSym e s,(Maybe s,CppType))
planSymSymEmbedding toS litType = SymEmbedding {
  embedLit= \e -> (mkLitPlanSym e,(Nothing,litType e)),
  unEmbed=planSymOrig . fst,
  symEq= \(x,_) (y,_) -> planSymOrig x == planSymOrig y,
  embedType=Just . snd . snd,
  embedInS= \(_,(s0,_)) s -> Just (toS s) == s0,
  embedIsLit= \case {NonSymbolName _ -> True; _ -> False} . planSymQnfName . fst
  }
data WrapEq e = WrapEq (e -> e -> Bool) e
instance Eq (WrapEq e) where
  WrapEq eq e == WrapEq eq' e' = eq e e' && eq' e e'
unWrapEq :: WrapEq e -> e
unWrapEq (WrapEq _ e) = e

eqSymEmbedding :: Eq e => SymEmbedding e [e] e
eqSymEmbedding = SymEmbedding {
  embedLit=id,
  unEmbed=id,
  symEq=(==),
  embedType=const $ Just CppNat,
  embedInS=elem,
  embedIsLit=const False
  }
