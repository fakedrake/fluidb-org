{-# LANGUAGE CPP                  #-}
{-# LANGUAGE DeriveFunctor        #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Data.Query.QuerySchema.Types
  (PlanSym(..)
  ,QueryPlan'(..)
  ,CppSchema'
  ,CppSchema
  ,QueryPlan
  ,ColumnProps(..)) where

import Data.CnfQuery.Types
import           Data.CppAst.CodeSymbol
import           Data.CppAst.CppType
import           Data.CppAst.Expression
import           Data.CppAst.ExpressionLike
import           Data.CppAst.Symbol
import qualified Data.List.NonEmpty         as NEL
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics
import           Text.Printf


data ColumnProps = ColumnProps {
  columnPropsCppType :: CppType,
  columnPropsConst   :: Bool
  }
  deriving (Show, Eq, Generic)
instance AShow ColumnProps
instance ARead ColumnProps
instance Hashable ColumnProps

type QueryPlan e s = QueryPlan' (PlanSym e s)
data QueryPlan' e' = QueryPlan {
  qpSchema :: [(e',ColumnProps)],
  qpUnique :: NEL.NonEmpty (NEL.NonEmpty e')
  } deriving (Show, Generic, Eq, Functor)
instance Hashables2 e s => Hashable (QueryPlan e s)
instance (AShow e, AShow s) => AShow (QueryPlan e s)
instance (Hashables2 e s, ARead e, ARead s) => ARead (QueryPlan e s)
data PlanSym e s = PlanSym {
  planSymCnfName     :: CNFName e s,
  planSymCnfOriginal :: e
  } deriving (Show, Generic)
-- Note: don't use the projection names as the projections are ofter
-- nreused with different names.
instance (Hashables2 e s, ExpressionLike e) => ExpressionLike (PlanSym e s) where
  toExpression PlanSym{..} = case planSymCnfName of
    Column c i       -> sym $ printf "sym_%d_%d" i (hash c)
    NonSymbolName e  -> toExpression e
    PrimaryCol e _ i -> sym $ toS e i
    where
      toS e i = case toExpression e of
        SymbolExpression (Symbol (CppSymbol x)) ->  x ++ "_" ++ show i
        x -> error $ "Expected symbol as primary column, not " ++ show x
      sym :: String -> Expression CodeSymbol
      sym = SymbolExpression . Symbol . CppSymbol
instance (Eq e, Eq s) => Eq (PlanSym e s) where
  a == b = planSymCnfName a == planSymCnfName b
instance (AShow e, AShow s) => AShow (PlanSym e s)
instance (Hashables2 e s, ARead e, ARead s) => ARead (PlanSym e s)
instance Hashables2 e s => Hashable (PlanSym e s) where
  hash = hash . planSymCnfName
  hashWithSalt s = hashWithSalt s . planSymCnfName
-- | We would use Int as the CppType underlying expression type but
-- schemas are not only table but also query schema.
type CppSchema' e = [(CppType, e)]
type CppSchema = CppSchema' CodeSymbol
