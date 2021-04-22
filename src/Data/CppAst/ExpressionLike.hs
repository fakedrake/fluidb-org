{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE UndecidableInstances  #-}

module Data.CppAst.ExpressionLike (ExpressionLike(..)) where

import           Data.CppAst.CodeSymbol
import           Data.CppAst.Expression
import           Data.CppAst.Symbol
import           Text.Printf

class ExpressionLike a where
  toExpression :: a -> Expression CodeSymbol

instance ExpressionLike (Integer,Integer) where
  toExpression (fr,to) = toExpression
    $ if fr == to then "prim" ++ show fr else printf "foreign%d_%d" fr to
instance ExpressionLike String where
  toExpression = SymbolExpression . Symbol . CppSymbol
