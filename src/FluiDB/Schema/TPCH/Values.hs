{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}
module FluiDB.Schema.TPCH.Values
  (tpchGlobalConf
  ,toQnfTpch
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

import           Control.Monad.State
import           Data.Bifunctor
import           Data.Codegen.Build.Types
import           Data.Codegen.SchemaAssocClass
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySize
import           Data.Query.SQL.QFile
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.ListT
import           Data.Utils.Unsafe
import           FluiDB.ConfValues
import           FluiDB.Schema.TPCH.Schemata
import           FluiDB.Types
import           Text.Printf


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
  fromRightErr
  $ mkGlobalConf
  $ PreGlobalConf
  { pgcExpIso = (id,id)
   ,pgcToUniq = asUniq
   ,pgcToQFile = id
   ,pgcPrimKeyAssoc = tpchSqlPrimKeys
   ,pgcSchemaAssoc = tpchSqlSchemaAssoc
   ,pgcTableSizeAssoc = tpchSqlTableSizes
   ,pgcBudget = Nothing
   ,pgcDataDir = "/tmp/fluidb_store"
  }
  where
    asUniq i = \case
      ESym e -> Just $ ESym $ printf "uniq_%s_%d" e i
      _      -> Nothing


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

toQnfTpch :: SqlTypeVars e s t n => Query e s -> QNFQuery e s
toQnfTpch q =
  fst
  $ fromJustErr
  $ either (error . ashow) id
  $ (`evalStateT` def)
  $ headListT
  $ toQNF (fmap2 snd . tableSchema (globalQueryCppConf tpchGlobalConf)) q
