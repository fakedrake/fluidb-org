module FluiDB.Bamify.DBGen (mkAllDataFiles) where

import           Control.Monad
import           Data.Codegen.Run
import           Data.Query.QuerySchema.Types
import           FluiDB.Bamify.Bamify
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
mkAllDataFiles :: DBGenConf -> IO ()
mkAllDataFiles DBGenConf{..} = do
  curDir <- getCurrentDirectory
  let checkExists = dbGenConfIncremental
  forM_ dbGenConfTables $ \DBGenTableConf{..} -> do
    schema <- getSchemaOrDie dbGenConfSchema dbGenTableConfFileBase
    let datFile = curDir </> dbGenTableConfFileBase <.> "dat"
    withExists checkExists datFile $ \bamaDir -> do
      runProc $ mkProc dbGenConfExec ["-s",show dbGenConfScale,"-T",[dbGenTableConfChar]]
      let tblFile = curDir </> dbGenTableConfFileBase <.> "tbl"
      let bamaFile = bamaDir </> dbGenTableConfFileBase <.> "bama"
      mkBamaFile schema tblFile bamaFile
      mkDataFile schema bamaFile datFile
  where
    withExists False _ m = tmpDir "mkTableDat" m
    withExists True datFile m = do
      exists <- doesFileExist datFile
      unless exists $ tmpDir "mkTableDat" m
