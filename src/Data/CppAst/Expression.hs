{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Data.CppAst.Expression
  ( Expression(..)
  , Type(..)
  , FunctionSymbol(..)
  , TmplInstArg(..)
  , clearTypeModifiers
  ) where

import           Data.CppAst.ClassSymbol
import           Data.CppAst.CppTypeF
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Operator
import           Data.CppAst.Symbol
import           Data.CppAst.TypeModifier
import           Data.List
import qualified Data.Set                        as DS
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

data FunctionSymbol a = SimpleFunctionSymbol (Symbol a)
                      | StaticMember (ClassSymbol a) (FunctionSymbol a)
                      | InstanceMember (Expression a) (FunctionSymbol a)
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (FunctionSymbol a)
instance AShow a => AShow (FunctionSymbol a)
instance ARead a => ARead (FunctionSymbol a)

data Expression a
  = SymbolExpression (Symbol a)
  | Parens (Expression a)
  | E2ession Operator (Expression a) (Expression a)
  | FunctionAp (FunctionSymbol a) [TmplInstArg a] [Expression a]
  | ObjectMember (Expression a) (Symbol a)
  | ClassStaticMember (Expression a) (Symbol a)
  | LiteralStringExpression String
  | LiteralIntExpression Int
  | LiteralBoolExpression Bool
  | LiteralFloatExpression Double
  | LiteralNullExpression
  | Quote String
  deriving (Eq,Show,Functor,Foldable,Traversable,Generic)
instance Hashable a => Hashable (Expression a)
instance AShow a => AShow (Expression a)
instance ARead a => ARead (Expression a)
instance Codegen a c => Codegen (Expression a) c where
  toCode = go where
    go = \case
      LiteralNullExpression -> "NULL"
      ClassStaticMember e s -> toCode e <> "::" <> toCode (runSymbol s)
      ObjectMember e s -> toCode e <> "." <> toCode (runSymbol s)
      SymbolExpression s -> toCode $ runSymbol s
      E2ession op e1 e2 -> go e1 <> " " <> toCode op <> " " <> go e2
      Parens e -> "(" <> go e <> ")"
      LiteralStringExpression s -> "\"" <> fromString (escape s) <> "\"" where
        escape xs = \case {
          '\n' -> "\\n";
          '\"' -> "\\\"";
          x    -> [x]} =<< xs
      LiteralIntExpression i -> toCode i
      LiteralFloatExpression i -> toCode i
      LiteralBoolExpression b -> toCode b
      Quote code -> fromString code
      FunctionAp fn targ arg -> toCode fn
                               <> templateArgs targ
                               <> "("
                               <> argCode arg
                               <> ")"
        where
          templateArgs targs = case targs of
            [] -> ""
            _  -> "<" <> argCode targ <> ">"
          argCode args =
            mconcat $ intersperse ", " $ toCode <$> args


data Type a
  = ClassType (DS.Set TypeModifier) [TmplInstArg a] (ClassSymbol a)
  | PrimitiveType (DS.Set TypeModifier) (CppTypeF (Expression a))
  deriving (Eq,Show,Functor,Foldable,Traversable,Generic)
instance Hashable a => Hashable (Type a)
instance AShow a => AShow (Type a)
instance ARead a => ARead (Type a)
instance Hashable a => Hashable (DS.Set a) where
  hashWithSalt i = hashWithSalt i . DS.toList
instance forall a c . Codegen a c => Codegen (Type a) c where
  toCode = \case
    PrimitiveType mods t -> modifyType (DS.toList mods)
                           $ (toCode :: CppTypeF (Expression a) -> c) t
    ClassType mods targ t ->
      modifyType (DS.toList mods) (toCode $ runSymbol t)
      <> if null targ then "" else "<" <> argCode targ <> ">"
    where
      modifyType :: [TypeModifier] -> c -> c
      modifyType = flip $ foldl $  \x -> \case
        Pointer   -> x <> "*"
        Reference -> x <> "&"
        CppConst  -> "const " <> x
        Static    -> "static " <> x
      argCode args =
        mconcat $ intersperse ", " $ toCode <$> args
clearTypeModifiers :: Type a -> Type a
clearTypeModifiers = \case
  ClassType _ targ x -> ClassType mempty targ x
  PrimitiveType _ x  -> PrimitiveType mempty x

data TmplInstArg a = TypeArg (Type a) | TemplArg (Expression a)
  deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (TmplInstArg a)
instance AShow a => AShow (TmplInstArg a)
instance ARead a => ARead (TmplInstArg a)
instance Codegen a c => Codegen (TmplInstArg a) c where
  toCode = \case
    TypeArg t  -> toCode t
    TemplArg e -> toCode e

instance IsString a => IsString (FunctionSymbol a) where
  fromString = SimpleFunctionSymbol . fromString
instance IsString a => IsString (Type a) where
  fromString = ClassType mempty [] . fromString
instance IsString a => IsString (Expression a) where
  fromString = SymbolExpression . fromString
instance (IsCode c, Codegen a c) => Codegen (FunctionSymbol a) c where
  toCode = go where
    go = \case
      SimpleFunctionSymbol s -> toCode $ runSymbol s
      StaticMember c s       -> toCode c <> "::" <> go s
      InstanceMember e s     -> toCode e <> "." <> go s
