{-# LANGUAGE CPP                  #-}
{-# LANGUAGE DeriveFunctor        #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
module Data.Query.QuerySchema.Types
  (ShapeSym(..)
  ,QueryShape'(..)
  ,QuerySize(..)
  ,CppSchema'
  ,CppSchema
  ,QueryShape
  ,Filtered(..)
  ,ColumnProps(..)
  ,putRowSize
  ,QueryShapeNoSize
  ,modSize
  ,modCertainty
  ,useCardinality,modCardinality,querySizeBytes,queryShapeBytes,setFilter,Filter(..)) where

import           Data.Copointed
import           Data.CppAst.CodeSymbol
import           Data.CppAst.CppType
import           Data.CppAst.Expression
import           Data.CppAst.ExpressionLike
import           Data.CppAst.Symbol
import qualified Data.List.NonEmpty         as NEL
import           Data.Pointed
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

type QueryShapeNoSize a e s = QueryShape' a (ShapeSym e s)
type QueryShape e s = QueryShape' QuerySize (ShapeSym e s)
data QuerySize = QuerySize { qsTables :: TableSize,qsCertainty :: Double }
  deriving (Show,Generic,Eq)
instance Hashable QuerySize
instance AShow QuerySize
instance ARead QuerySize
querySizeBytes :: QuerySize -> Bytes
querySizeBytes QuerySize{..} = tableBytes qsTables
-- | Use all the properties of the second argument except
-- cardinality. Use the cardinality with the most certain. Also use the qsCa
useCardinality :: QuerySize -> QuerySize -> QuerySize
useCardinality qsCard qs =
  if qsCertainty qs >= qsCertainty qsCard then qs else qsCard
    { qsTables = go (qsTables qsCard) (qsTables qs) }
  where
    go tsCard ts = ts { tsRows = tsRows tsCard }
modCardinality :: (Cardinality -> Cardinality) -> QuerySize -> QuerySize
modCardinality card qs =
  qs { qsTables = (qsTables qs) { tsRows = card $ tsRows $ qsTables qs } }

modCertainty :: (Double -> Double) -> QuerySize -> QuerySize
modCertainty f qs = qs { qsCertainty = f $ qsCertainty qs }

modSize :: (s0 -> s1) -> QueryShape' s0 e -> QueryShape' s1 e
modSize f QueryShape {..} = QueryShape { qpSize = f qpSize,.. }
data Filter = FltSelect Double | FltNone | FltSingle deriving (Generic,Show,Eq)
data Filtered e = Filtered { fltFilter :: Filter,fltSymbol :: e }
  deriving (Generic,Show,Functor,Traversable,Foldable,Eq)
instance Hashable Filter
instance AShow Filter
instance ARead Filter

setFilter :: Filter -> Filtered a -> Filtered a
setFilter percent flt  = flt {fltFilter = percent}
instance Hashable e => Hashable (Filtered e) where
instance AShow e => AShow (Filtered e)
instance ARead e => ARead (Filtered e)
instance Pointed Filtered where
  point a = Filtered { fltFilter = FltNone,fltSymbol = a }
instance Copointed Filtered where
  copoint = fltSymbol

data QueryShape' sizeType e' =
  QueryShape
  { qpSchema :: [(e',ColumnProps)]
   ,qpUnique :: NEL.NonEmpty (NEL.NonEmpty (Filtered e'))
   ,qpSize   :: sizeType
  }
  deriving (Show,Generic,Eq,Functor)

-- | To determine the columns etc use the leftmost, for size inference
-- choose the most certain.
instance Semigroup (QueryShape size e') where
  a <> b =
    a { qpSize = if qsCertainty (qpSize a)
          < qsCertainty (qpSize b) then qpSize b else qpSize a
      }

instance Hashables2 e s => Hashable (QueryShape e s)
instance (AShow e, AShow s) => AShow (QueryShape e s) where
  ashow' = const $ Sym "QueryShape"
instance (Hashables2 e s, ARead e, ARead s) => ARead (QueryShape e s)
queryShapeBytes :: QueryShape e s -> Bytes
queryShapeBytes QueryShape{..} = querySizeBytes qpSize

putRowSize :: Bytes -> QuerySize -> QuerySize
putRowSize rs qs = qs { qsTables = (qsTables qs) { tsRowSize = rs } }

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
