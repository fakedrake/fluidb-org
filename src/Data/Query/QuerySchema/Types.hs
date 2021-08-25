{-# LANGUAGE CPP                  #-}
{-# LANGUAGE DeriveFunctor        #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Data.Query.QuerySchema.Types
  (ShapeSym(..)
  ,QueryShape'(..)
  ,QuerySize(..)
  ,CppSchema'
  ,CppSchema
  ,QueryShape
  ,ColumnProps(..)) where

import           Data.CppAst.CodeSymbol
import           Data.CppAst.CppType
import           Data.CppAst.Expression
import           Data.CppAst.ExpressionLike
import           Data.CppAst.Symbol
import qualified Data.List.NonEmpty         as NEL
import           Data.QnfQuery.Types
import           Data.Query.QuerySize
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

type QueryShape e s = QueryShape' (ShapeSym e s)
data QuerySize = QuerySize { qsTables :: [TableSize],qsCertainty :: Double }
  deriving (Show,Generic,Eq)
instance Hashable QuerySize
instance AShow QuerySize
instance ARead QuerySize

data QueryShape' e' =
  QueryShape
  { qpSchema :: [(e',ColumnProps)]
   ,qpUnique :: NEL.NonEmpty (NEL.NonEmpty e')
   ,qpSize   :: QuerySize
  }
  deriving (Show,Generic,Eq,Functor)
instance Hashables2 e s => Hashable (QueryShape e s)
instance (AShow e, AShow s) => AShow (QueryShape e s)
instance (Hashables2 e s, ARead e, ARead s) => ARead (QueryShape e s)

-- | Unique symbols
data ShapeSym e s = ShapeSym {
  shapeSymQnfName     :: QNFName e s,
  shapeSymQnfOriginal :: e
  } deriving (Show, Generic)
-- Note: don't use the projection names as the projections are ofter
-- nreused with different names.
instance (Hashables2 e s, ExpressionLike e) => ExpressionLike (ShapeSym e s) where
  toExpression ShapeSym{..} = case shapeSymQnfName of
    Column c i       -> sym $ printf "sym_%d_%d" i (hash c)
    NonSymbolName e  -> toExpression e
    PrimaryCol e _ i -> sym $ toS e i
    where
      toS e i = case toExpression e of
        SymbolExpression (Symbol (CppSymbol x)) ->  x ++ "_" ++ show i
        x -> error $ "Expected symbol as primary column, not " ++ show x
      sym :: String -> Expression CodeSymbol
      sym = SymbolExpression . Symbol . CppSymbol
instance (Eq e, Eq s) => Eq (ShapeSym e s) where
  a == b = shapeSymQnfName a == shapeSymQnfName b
instance (AShow e, AShow s) => AShow (ShapeSym e s)
instance (Hashables2 e s, ARead e, ARead s) => ARead (ShapeSym e s)
instance Hashables2 e s => Hashable (ShapeSym e s) where
  hash = hash . shapeSymQnfName
  hashWithSalt s = hashWithSalt s . shapeSymQnfName
-- | We would use Int as the CppType underlying expression type but
-- schemas are not only table but also query schema.
type CppSchema' e = [(CppType, e)]
type CppSchema = CppSchema' CodeSymbol
