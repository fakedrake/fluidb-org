{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}
module FluiDB.Schema.TPCH.Values
  (tpchGlobalConf
  ,toFS
  ,toCnfTpch
  ,resourcesDir
  ,bamaFile
  ,tblFile
  ,datFile
  ,tpchFileColumns
  ,tpchFileCache
  ,tpchSqlSchemaAssoc
  ,tpchSqlTableSizes
  ,tpchSqlPrimKeys
  ,cppFile
  ,exeFile) where

import Data.Bifunctor
import Data.Codegen.Build.Types
import Data.Utils.Default
import Control.Monad.State
import Data.Utils.ListT
import Data.CnfQuery.Build
import Data.Utils.AShow
import Data.CnfQuery.Types
import Data.Query.Algebra
import FluiDB.Types
import Data.Utils.Unsafe
import FluiDB.Schema.TPCH.Schemata
import Data.Utils.Functors
import Text.Printf
import FluiDB.ConfValues
import Data.Query.SQL.Types
import Data.Query.SQL.FileSet
import Data.Query.QuerySize
import Data.Codegen.SchemaAssocClass


tpchSqlSchemaAssoc :: SqlTypeVars e s t n => SchemaAssoc e s
tpchSqlSchemaAssoc = bimap (Just . DataFile . datFile) (fmap2 ESym)
  <$> tpchSchemaAssoc
tpchSqlTableSizes :: SqlTypeVars e s t n => [(s, TableSize)]
tpchSqlTableSizes = first (Just . DataFile . datFile) <$> tpchTableSizes
tpchSqlPrimKeys :: SqlTypeVars e s t n => [(s, [e])]
tpchSqlPrimKeys = (Just . DataFile . datFile) `bimap` fmap ESym
                  <$> tpchTablePrimKeys
tpchFileCache :: SqlTypeVars e s t n => QueryFileCache e s
tpchFileCache = mkFileCache id tpchSqlSchemaAssoc
tpchGlobalConf :: SqlTypeVars e s t n => GlobalConf e s t n
tpchGlobalConf =
  fromJustErr
  $ mkGlobalConf
    (id,id)
    asUniq
    id
    tpchSqlPrimKeys
    tpchSqlSchemaAssoc
    tpchSqlTableSizes
  where
    asUniq i = \case
      ESym e -> Just $ ESym $ printf "uniq_%s_%d" e i
      _ -> Nothing


cppFile :: Int -> FilePath
cppFile = printf (resourcesDir "tpch_query%03d.cpp")
exeFile :: Int -> FilePath
exeFile = printf (resourcesDir "tpch_query%03d.exe")

resourcesDir :: FilePath -> FilePath
#ifdef __APPLE__
resourcesDir = ("/Users/drninjabatman/Projects/UoE/FluiDB/resources/" ++)
#else
resourcesDir = ("/home/drninjabatman/Projects1/FluiDB/resources/" ++)
#endif
tpchFileColumns :: SqlTypeVars e s t n => s -> Maybe [e]
tpchFileColumns = toTableColumns tpchSqlSchemaAssoc

bamaFile :: Table -> FilePath
bamaFile = resourcesDir . printf "bamify/%s.bama" . unTable
tblFile :: Table -> FilePath
tblFile = resourcesDir . printf "tables/%s.tbl" . unTable
datFile :: Table -> FilePath
datFile = resourcesDir . printf "tables/%s.dat" . unTable
class FileSetLike x where toFS :: x -> FileSet
instance FileSetLike FilePath where toFS = DataFile . datFile . TSymbol
instance FileSetLike (FilePath,FilePath) where
  toFS = uncurry DataAndSet . bimap (datFile . TSymbol) (datFile . TSymbol)

toCnfTpch :: SqlTypeVars e s t n => Query e s -> CNFQuery e s
toCnfTpch q = fst
  $ fromJustErr
  $ either (error . ashow) id
  $ (`evalStateT` def)
  $ headListT
  $ toCNF (fmap2 snd . tableSchema (globalQueryCppConf tpchGlobalConf))
  q
