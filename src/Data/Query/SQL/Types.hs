{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE ViewPatterns          #-}

module Data.Query.SQL.Types
  ( zeroDay
  , dateToInteger
  , integerToDate
  , negDate
  , epoc
  , Table(..)
  , SQLQuery
  , Date(..)
  , ExpTypeSym'(..)
  , ExpTypeSym
  , Symbolic(..)
  , NestedQueryE(..)
  , ExtractSymbols(..)
  , unTable
  , isSymbol
  ) where

import           Control.Monad.State
import           Data.Bifunctor
import           Data.CppAst.Expression
import           Data.CppAst.ExpressionLike
import           Data.Foldable
import           Data.Maybe
import           Data.Query.Algebra
import           Data.String
import           Data.Time.Calendar
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Hashable
import           GHC.Generics

-- | An endomorphism.
data Table
  = NoTable
  | TSymbol String
  deriving (Show,Eq,Generic,Ord,Read)

unTable :: Table -> String
unTable (TSymbol s) = s
unTable _           = error "No string associated with table."
instance IsString Table where
  fromString = TSymbol
type SQLQuery s = Query ExpTypeSym s

instance Hashable Table

data Date =
  Date
  { year    :: Integer
   ,month   :: Integer
   ,day     :: Integer
   ,hour    :: Integer
   ,minute  :: Integer
   ,seconds :: Integer
  }
  deriving (Show,Eq,Generic)
instance AShow Date
instance ARead Date

instance Hashable Date
zeroDay :: Date
zeroDay = Date 0 0 0 0 0 0
epoc :: Date
epoc = Date 1970 1 1 0 0 0

type ExpTypeSym = ExpTypeSym' String
-- instance IsString e => IsString (ExpTypeSym' e) where
--   fromString = ESym . fromString

data ExpTypeSym' e
  = EDate Date
  | EInterval Date
  | EFloat Double
  | EInt Integer
  | EString String
  | ESym e
  | EBool Bool
  | ECount (Maybe e)
  deriving (Show,Eq,Generic,Functor,Traversable,Foldable)
instance Default (ExpTypeSym' e) where def = EBool False
instance AShow e => AShow (ExpTypeSym' e)
instance ARead e => ARead (ExpTypeSym' e)

instance Hashable e => Hashable (ExpTypeSym' e)

class Symbolic e where
  type SymbolType e :: *
  asSymbol :: e -> Maybe (SymbolType e)
  mkSymbol :: SymbolType e -> e
  default mkSymbol :: (Applicative a, SymbolType s ~ SymbolType (a s),Symbolic s, e ~ a s) => SymbolType e -> e
  mkSymbol = pure . mkSymbol

instance Symbolic s => Symbolic (Query e s) where
  type SymbolType (Query e s) = SymbolType s
  asSymbol = \case {Q0 s -> asSymbol s;_ -> Nothing}
instance Symbolic s => Symbolic (Prop s) where
  type SymbolType (Prop s) = SymbolType s
  asSymbol = \case {P0 s -> asSymbol s;_ -> Nothing}
instance Symbolic s => Symbolic (Aggr s) where
  type SymbolType (Aggr s) = SymbolType s
  asSymbol = \case {NAggr AggrFirst s -> asSymbol s;_ -> Nothing}
instance Symbolic s => Symbolic (Rel s) where
  type SymbolType (Rel s) = SymbolType s
  asSymbol = \case {R0 s -> asSymbol s;_ -> Nothing}
instance Symbolic s => Symbolic (Expr s) where
  type SymbolType (Expr s) = SymbolType s
  asSymbol = \case {E0 s -> asSymbol s;_ -> Nothing}
instance Symbolic s => Symbolic (Maybe s) where
  type SymbolType (Maybe s) = SymbolType s
  asSymbol = \case {Just s -> asSymbol s;_ -> Nothing}
instance Symbolic () where
  type SymbolType () = ()
  asSymbol = Just
  mkSymbol = const ()

instance Symbolic FilePath where
  type SymbolType FilePath = FilePath
  asSymbol = Just
  mkSymbol = id

instance Symbolic (ExpTypeSym' e) where
  type SymbolType (ExpTypeSym' e) = e
  asSymbol = \case
    ESym s -> Just s
    _      -> Nothing
  mkSymbol = ESym

instance Symbolic Table where
  type SymbolType Table = String
  asSymbol = \case {TSymbol s -> Just s;_ -> Nothing}
  mkSymbol = TSymbol

instance AShow Table
instance ARead Table

newtype NestedQueryE s e =
  NestedQueryE { runNestedQueryE :: Either (Query (NestedQueryE s e) s) e }
  deriving (Eq,Show,Generic)

instance Bifunctor NestedQueryE where
  bimap f g = NestedQueryE
    . bimap (bimap (bimap f g) f) g
    . runNestedQueryE

instance AShow2 e s => AShow (NestedQueryE s e)

instance Foldable (NestedQueryE s) where
  foldMap f (NestedQueryE (Left q))  = foldMap (foldMap f) $ FlipQuery q
  foldMap f (NestedQueryE (Right e)) = f e

instance Traversable (NestedQueryE s) where
  traverse f (NestedQueryE (Left q)) =
    NestedQueryE . Left .  unFlipQuery <$> traverse (traverse f) (FlipQuery q)
  traverse f (NestedQueryE (Right e)) = NestedQueryE . Right <$> f e

instance Functor (NestedQueryE s) where
  fmap f (NestedQueryE (Left q)) = NestedQueryE $ Left
    $ unFlipQuery $ fmap f <$>  FlipQuery q
  fmap f (NestedQueryE (Right e)) = NestedQueryE $ Right $ f e

instance Symbolic e => Symbolic (NestedQueryE s e) where
  type SymbolType (NestedQueryE s e) = SymbolType e
  asSymbol = \case {NestedQueryE (Right e) -> asSymbol e;_ -> Nothing}
  mkSymbol = NestedQueryE . Right . mkSymbol

isSymbol :: Symbolic e => e -> Bool
isSymbol = isJust . asSymbol
class ExtractSymbols s a where
  extractSymbols :: a -> [s]

instance (Foldable t1, ExtractSymbols s (t2 e)) =>
         ExtractSymbols s (t1 (t2 e)) where
  extractSymbols = (>>= extractSymbols) . toList

instance ExtractSymbols String String where
  extractSymbols x = [x]
dateToInteger :: Date -> Integer
dateToInteger (Date y m d h mm s) = ((24 * days + h) * 60 + mm) * 60 + s
  where
    days = diffDays (fromGregorian y (fromInteger m) (fromInteger d)) epocInt
    epocInt = fromGregorian 1970 0 0

negDate :: Date -> Date
negDate (Date a b c d e f) = Date (-a) (-b) (-c) (-d) (-e) (-f)

integerToDate :: Integer -> Date
integerToDate = evalState go where
  go = do
    seconds <- extractLastN 60
    minute <- extractLastN 60
    hour <- extractLastN 24
    allDays <- get
    let (year, toInteger -> month, toInteger -> day) =
          toGregorian $ allDays `addDays` fromGregorian 1970 0 0
    return Date{..}
  extractLastN m = do
    x <- get
    modify (`div` m)
    return $ x `mod` m
instance ExpressionLike e => ExpressionLike (ExpTypeSym' e) where
  toExpression = \case
    EDate d -> LiteralIntExpression $ fromInteger $ dateToInteger d
    EInterval _ -> -- 3 month interval depends on the date.
      error "Date intervals should have been squashed into dates at preproc."
    EFloat d -> LiteralFloatExpression d
    EInt d -> LiteralIntExpression $ fromInteger d
    EString s -> LiteralStringExpression s
    ESym e -> toExpression e
    EBool b -> LiteralBoolExpression b
    ECount _ ->
      error "Count expressions should have been converted to aggregation functions."
