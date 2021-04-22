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

module Database.FluiDB.Running.Bamify
  ( writeResultIO
  , getSchemaOrDie
  , tblToBama
  , bamaToDat
  ) where

import           Control.Monad.Morph
import           Control.Monad.State
import qualified Data.ByteString.Lazy                       as BS
import           Data.Void
import           Database.FluiDB.AShow
import           Database.FluiDB.Codegen.Build
import           Database.FluiDB.Codegen.Examples.TblParser
import           Database.FluiDB.CppAst                     as CC
import           Database.FluiDB.Relational.Parser.Types
import           Database.FluiDB.Relational.TpchSchemata
import           Database.FluiDB.Running.Classes
import           Database.FluiDB.Running.Code
import           Database.FluiDB.Running.TpchValues
import           Database.FluiDB.Running.Types
import           Database.FluiDB.Utils
import           System.Exit
import           System.FilePath.Posix
import           System.IO
import           Text.Megaparsec                            hiding (empty)

writeResultIO :: Handle
              -> Either (ParseErrorBundle BS.ByteString Void) BS.ByteString
              -> IO ()
writeResultIO fd res = case res of
  Left e        -> die $ show e
  Right binLine -> BS.hPut fd binLine

getSchemaOrDie :: Table -> IO CppSchema
getSchemaOrDie table = case lookup table tpchSchemaAssoc of
  Nothing     -> die $ "Couldn't find schema for table: " ++ show table
  Just schema -> return $ fmap2 CC.CppSymbol schema

-- Turn a table into bama limiting
tblToBama :: MonadFakeIO m =>
            Maybe Int
          -> FilePath
          -> FilePath
          -> GlobalSolveT e s t n m ()
tblToBama limit tblFN bamaFN = do
  logMsg $ tblFN ++ " -> " ++ bamaFN ++ " limit: " ++ show limit
  safeFileTbl <- existing tblFN
  liftFakeIO $ do
    fileContents <- BS.readFile safeFileTbl
    schema <- getSchemaOrDie $ TSymbol $ takeBaseName tblFN
    fd <- openBinaryFile bamaFN WriteMode
    let eitherRes = parseLines schema safeFileTbl fileContents limit
    units <- mapM (writeResultIO fd) eitherRes
    hClose fd
    logMsg $ "Lines: " <> show (length units)

bamaToDat :: forall e s t n m .
            (ExpressionLike e, Hashables2 e s, AShow e, AShow s,
             MonadFakeIO m, MonadPlus m) =>
            QueryPlan e s
          -> FilePath
          -> FilePath
          -> GlobalSolveT e s t n m ()
bamaToDat q bam dat = do
  logMsg $ bam ++ " -> " ++ dat
  cppCode <- bamToDatCode
  let cpp = dat ++ ".cpp"
  let exe = dat ++ ".exe"
  writeFileFake cpp cppCode
  compileAndRunCppFile cpp exe
  inDir (resourcesDir "tpch_data/") $ void $ cmd exe []
  where
    bamToDatCode = runCodeBuild $ lift $ do
      tellInclude $ CC.LocalInclude $ resourcesDir "include/bamify.hh"
      tellInclude $ CC.LocalInclude $ resourcesDir "include/common.hh"
      tellInclude $ CC.LibraryInclude "sstream"
      tellInclude $ CC.LibraryInclude "array"
      tellInclude $ CC.LibraryInclude "string"
      cls <-  (`evalStateT` 0)
        (queryRecord q :: StateT Int (CodeBuilderT e s t n m) (Class CodeSymbol))
      getCppProgram [
        CC.ExpressionSt
        $ CC.FunctionAp "bama_to_dat"
          [CC.TemplArg $ CC.SymbolExpression $ CC.classNameRef cls]
          [CC.LiteralStringExpression bam,
           CC.LiteralStringExpression dat]]
