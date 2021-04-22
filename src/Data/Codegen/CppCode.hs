{-# LANGUAGE ConstraintKinds       #-}
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
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Data.Codegen.CppCode
  ( Symbol(..)
  , CodeSymbol(..)
  , Operator(..)
  , ClassSymbol
  , Type(..)
  , FunctionSymbol(..)
  , Expression(..)
  , Assignment(..)
  , Statement(..)
  , Argument(..)
  , Declaration(..)
  , Function(..)
  , Constructor(..)
  , Include(..)
  , Member(..)
  , Class(..)
  , Template(..)
  , TmplDefArg(..)
  , TmplInstArg(..)
  , CodegenIndent(..)
  , TypeModifier(..)
  , toCodeIndent
  , clearTypeModifiers
  , normalizeCode
  , CodeCache
  , symbolRef
  , declarationNameRef
  , functionNameRef
  , classNameRef
  , TypeDef(..)
  , classDeclaration
  , runCodeCache
  , coutSt
  -- Move the code for the following here.
  , CppTypeF(..)
  , CppType
  , QueryFileCache(..)
  , CppSize(..)
  , toCode
  , sortDefs
  , constString
  ) where

import Data.CppAst.CppTypeF
import           Data.CppAst.Argument
import           Data.CppAst.Assignment
import           Data.CppAst.Class
import           Data.CppAst.ClassDeclaration
import           Data.CppAst.ClassSymbol
import           Data.CppAst.CodeSymbol
import           Data.CppAst.Constructor
import           Data.CppAst.Declaration
import           Data.CppAst.Expression
import           Data.CppAst.Function
import           Data.CppAst.Include
import           Data.CppAst.LiteralCode.CodeCache
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.Member
import           Data.CppAst.Operator
import           Data.CppAst.Statement
import           Data.CppAst.Symbol
import           Data.CppAst.Template
import           Data.CppAst.TmplDefArg
import           Data.CppAst.TypeDef
import           Data.CppAst.TypeModifier
import Data.Query.SQL.FileSet

type CppType = CppTypeF (Expression CodeSymbol)
