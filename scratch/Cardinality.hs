{-# LANGUAGE UndecidableInstances #-}
module Data.Query.Optimizations.Cardinality (Cardinality(..)) where

import           Data.Codegen.Build.Types
import           Data.Query.Algebra
import           Data.Utils.AShow
import           Data.Utils.Default
import           GHC.Generics

data Cardinality e s
  = Cardinality
  | EqCard [(e,e)] (Cardinality e s,Query e s) (Cardinality e s,Query e s)
  | CardProd (Cardinality e s) (Cardinality e s)
  | CardSel (Cardinality e s)
  | CardEq (Cardinality e s)
  | CardUnion (Cardinality e s) (Cardinality e s)
  | CardGrp (Cardinality e s)
  | CardDrop Int (Cardinality e s)
  | CardQ s
  | CardN Int
  deriving Generic
instance AShowV2 e s => AShow (Cardinality e s)
instance Default (Cardinality e s) where
  def = Cardinality

calcCardinality
  :: (Query e (QueryShape e s) -> QueryShape e s0) -> (s -> QueryShape e s) -> Cardinality e s -> Int
calcCardinality isIn symSize = \case
  EqCard es (ls,l) (rs,r) -> allUniques es
  CardProd _ _            -> _
  CardSel _               -> _
  CardEq _                -> _
  CardUnion _ _           -> _
  CardGrp _               -> _
  CardDrop _ _            -> _
  CardQ s                 -> symSize s
  CardN n                 -> n


-- Extras
--
eqJoinCard
  :: [e] -> FuzzyQuery e s -> FuzzyQuery e s -> Cardinality e s
eqJoinCard a l r =
  EqCard a (getCardinality $ Q0 l,unFuzz l) (getCardinality $ Q0 r,unFuzz r)
