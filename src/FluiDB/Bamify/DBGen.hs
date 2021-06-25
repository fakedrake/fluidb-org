module FluiDB.Bamify.DBGen (mkAllDataFiles) where

import           Control.Monad
import           Data.Codegen.Run
import           Data.Query.QuerySchema.Types
import           Data.Query.QuerySize
import           Data.Query.SQL.Types
import           Data.Utils.Debug
import           FluiDB.Bamify.CsvParse
import           FluiDB.Bamify.Types
import           FluiDB.Bamify.Unbamify
import           System.Directory
import           System.FilePath

getSchemaOrDie :: [(String,CppSchema)] -> String -> IO CppSchema
getSchemaOrDie schemaAssoc tableName =
  case lookup tableName schemaAssoc of
    Nothing     -> fail $ "Couldn't find schema for table: " ++ tableName
    Just schema -> return schema


-- | Runs dbgen to build all in the current directory.
mkAllDataFiles :: DBGenConf -> IO [(Table,TableSize)]
mkAllDataFiles DBGenConf{..} = do
  curDir <- getCurrentDirectory
  let checkExists = dbGenConfIncremental
  forM dbGenConfTables $ \tbl@DBGenTableConf{..} -> do
    schema <- getSchemaOrDie dbGenConfSchema dbGenTableConfFileBase
    when (null schema) $ fail $ "Can't deal with empty schema for: " ++ ashow tbl
    let datFile = curDir </> dbGenTableConfFileBase <.> "dat"
    let sizeFile = datFile <.> "size"
    let readQuerySize = read <$> readFile sizeFile
    withExists readQuerySize checkExists datFile $ tmpDir "dbgen" $ \td -> do
      traceM $ printf "Generating: %s" dbGenTableConfFileBase
      runProc $ mkProc dbGenConfExec ["-s",show dbGenConfScale,"-T",[dbGenTableConfChar]]
      let tblFile = td </> dbGenTableConfFileBase <.> "tbl"
      let bamaFile = td </> dbGenTableConfFileBase <.> "bama"
      traceM $ printf "Bamify %s %s " tblFile bamaFile
      tableSize <- bamifyFile (fst <$> schema) tblFile bamaFile
      traceM $ printf "Unamify %s %s" bamaFile datFile
      mkDataFile schema bamaFile datFile
      let symSize = (TSymbol dbGenTableConfFileBase,tableSize)
      writeFile sizeFile $ show symSize
      return symSize
  where
    withExists _ False _ m = m
    withExists a True datFile m = do
      exists <- doesFileExist datFile
      if exists then a else m
