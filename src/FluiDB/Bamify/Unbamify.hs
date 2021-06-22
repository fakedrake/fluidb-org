{-# LANGUAGE OverloadedStrings #-}
module FluiDB.Bamify.Unbamify
  (mkDataFile) where

import           Data.Codegen.Run
import           Data.CppAst.Argument
import           Data.CppAst.Class
import           Data.CppAst.CodeSymbol
import           Data.CppAst.CppType
import           Data.CppAst.Declaration
import           Data.CppAst.Expression
import           Data.CppAst.Function
import           Data.CppAst.Include
import           Data.CppAst.LiteralCode.Codegen
import           Data.CppAst.LiteralCode.SoftUNameCode
import           Data.CppAst.RecordCls
import           Data.CppAst.Statement
import           Data.CppAst.TypeModifier
import           Data.Query.QuerySchema.Types
import           Data.Query.QuerySize
import           Data.Query.SQL.Types
import qualified Data.Set                              as DS
import           FluiDB.Bamify.Types

-- | Turn a table file (csv file generated from dbgen) into a
mkDataFile :: CppSchema -> BamaFile -> DataFile -> IO ()
mkDataFile [] _bamaFile _dataFile =
  error "can't make data file on an empty schema."
mkDataFile schema bamaFile dataFile = tmpDir "mkDataFile" $ \_ -> do
  unBamifyCode schema bamaFile dataFile >>= runCpp

-- | Code to read the bama file and output a paged file that is
-- readable by fluidb plans.
unBamifyCode :: CppSchema -> BamaFile -> DataFile -> IO CppCode
unBamifyCode [] _bamaFile _dataFile = error "Can't unbamify an empty schema"
unBamifyCode schema bamaFile dataFile = do
  let codeTroika = bamifyCode schema bamaFile dataFile
  return $ troikaToCode codeTroika
  where
    troikaToCode :: ([Include],Class CodeSymbol,Function CodeSymbol) -> String
    troikaToCode (incs,r,mainCode) =
      literalCodeToString
      $ mconcat (fmap toCodeIndent incs)
      <> toCodeIndent r
      <> toCodeIndent mainCode

bamifyCode
  :: CppSchema
  -> BamaFile
  -> DataFile
  -> ([Include],Class CodeSymbol,Function CodeSymbol)
bamifyCode schema inFile outFile =
  (incs
  ,recordCls "Record" schema
  ,Function
   { functionName = "main"
    ,functionType = PrimitiveType mempty CppInt
    ,functionBody =
       [ExpressionSt
        $ Quote
        $ "bama_to_dat<Record>(" <> show inFile <> "," <> show outFile <> ")"] -- mainFn inFile outFile recNum
    ,functionArguments = Argument
       <$> [Declaration
            { declarationName = "argc"
             ,declarationType = PrimitiveType mempty CppInt
            }
           ,Declaration
            { declarationName = "argv[]"
             ,declarationType = PrimitiveType (DS.singleton Pointer) CppChar
            }]
    ,functionConstMember = False
   })
  where
    incs = [LibraryInclude "bamify.hh"]
