{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}

module Data.CppAst.Function (
  Function(..), functionNameRef
  ) where

import           Data.CppAst.Argument
import           Data.CppAst.CodeSymbol
import           Data.CppAst.Expression
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Statement
import           Data.CppAst.Symbol
import           Data.List
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

putLinesIndented :: Codegen a c => [Statement a] -> Offset c ()
putLinesIndented body = withOffset 2 $ mapM_ toCodeIndent' body
functionNameRef :: Function CodeSymbol -> Symbol CodeSymbol
functionNameRef = fmap symbolRef . functionName
data Function a = Function {
  functionName        :: Symbol a,
  functionType        :: Type a,
  functionBody        :: [Statement a],
  functionArguments   :: [Argument a],
  functionConstMember :: Bool
  } deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Function a)
instance AShow a => AShow (Function a)
instance Codegen a c => CodegenIndent (Function a) c where
  toCodeIndent' Function{..} = do
    putLine $ toCode functionType <> " "
      <> toCode functionName <> "("
      <> mconcat (intersperse ", " $ toCode <$> functionArguments)
      <> ") " <> (if functionConstMember then "const" else "") <> "{"
    putLinesIndented functionBody
    putLine "}"
