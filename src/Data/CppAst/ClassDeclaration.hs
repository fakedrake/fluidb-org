{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}

module Data.CppAst.ClassDeclaration
  ( ClassDeclaration(..)
  , classDeclaration
  ) where

import           Data.CppAst.Class
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Symbol
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics                    (Generic)

data ClassDeclaration a = ClassDeclaration {
  classDeclarationName :: Symbol a
  } deriving (Eq, Show, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (ClassDeclaration a)
instance AShow a => AShow (ClassDeclaration a)
classDeclaration :: Class a -> ClassDeclaration a
classDeclaration Class{..} = ClassDeclaration{classDeclarationName=className}
instance (IsCode c, Codegen a c) => CodegenIndent (ClassDeclaration a) c where
  toCodeIndent' ClassDeclaration{..} =
    putLine $ "class " <> toCode classDeclarationName <> ";"
