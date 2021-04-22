{-# LANGUAGE TypeFamilies #-}
module Database.FluiDB.Running.WisconsinValues
  ( wisconsinGlobalConf
  , wisconsinFileColumns
  , wisconsinFileCache
  , wisconsinSqlSchemaAssoc
  , wisconsinSqlSizes
  , wisconsinSqlPrimKeys
  ) where

import           Data.Bifunctor
import           Database.FluiDB.Codegen.SchemaAssocClass
import           Database.FluiDB.FileSet
import           Database.FluiDB.QuerySize
import           Database.FluiDB.Relational.Parser.Types
import           Database.FluiDB.Relational.WisconsinSchemata
import           Database.FluiDB.Running.ConfValues
import           Database.FluiDB.Running.TpchValues
import           Database.FluiDB.Running.Types
import           Database.FluiDB.Utils

wisconsinSqlSchemaAssoc :: SqlTypeVars e s t n => SchemaAssoc e s
wisconsinSqlSchemaAssoc = bimap (Just . DataFile . datFile) (fmap2 ESym)
                          <$> wisconsinSchemaAssoc
wisconsinSqlSizes :: SqlTypeVars e s t n => [(s, TableSize)]
wisconsinSqlSizes = first (Just . DataFile . datFile) <$> wisconsinSizes
wisconsinSqlPrimKeys :: SqlTypeVars e s t n => [(s, [e])]
wisconsinSqlPrimKeys = (Just . DataFile . datFile)
                       `bimap` fmap ESym
                  <$> wisconsinPrimKeys
wisconsinFileCache :: SqlTypeVars e s t n => QueryFileCache e s
wisconsinFileCache = mkFileCache id wisconsinSqlSchemaAssoc
wisconsinGlobalConf :: SqlTypeVars e s t n => GlobalConf e s t n
wisconsinGlobalConf = fromJustErr $ mkGlobalConf
  (id,id)
  (\_ _ -> Nothing)
  id
  wisconsinSqlPrimKeys
  wisconsinSqlSchemaAssoc
  wisconsinSqlSizes


wisconsinFileColumns :: SqlTypeVars e s t n => s -> Maybe [e]
wisconsinFileColumns = toTableColumns wisconsinSqlSchemaAssoc
