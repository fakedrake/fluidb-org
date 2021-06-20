{-# LANGUAGE OverloadedStrings #-}
module FluiDB.Bamify.Unbamify
  (mkDataFile) where

import           Data.Codegen.CppType
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
import           Data.CppAst.Symbol
import           Data.CppAst.TypeModifier
import           Data.Query.QuerySchema.Types
import qualified Data.Set                              as DS
import           FluiDB.Bamify.Types
import           System.Exit
import           System.Posix

-- | Turn a table file (csv file generated from dbgen) into a
mkDataFile :: CppSchema -> BamaFile -> DataFile -> IO ()
mkDataFile schema bamaFile dataFile = tmpDir "mkDataFile" $ \_ -> do
  unBamifyCode schema bamaFile dataFile >>= runCpp

-- | Code to read the bama file and output a paged file that is
-- readable by fluidb plans.
unBamifyCode :: CppSchema -> BamaFile -> DataFile -> IO CppCode
unBamifyCode schema bamaFile dataFile = do
  size <- fromIntegral . fileSize <$> getFileStatus bamaFile
  paddings <- maybe (die "Couldn't deduce paddings for schema") return
    $ schemaPostPaddings
    $ fst <$> schema
  sizes <- maybe (die "Couldn't deduce paddings for schema") return
    $ traverse (cppTypeSize . fst) schema
  let recordSize = sum sizes + sum paddings
  let codeTroika = bamifyCode (size `div` recordSize) bamaFile dataFile schema
  return $ troikaToCode codeTroika
  where
    troikaToCode :: ([Include],Class CodeSymbol,Function CodeSymbol) -> String
    troikaToCode (incs,r,mainCode) =
      literalCodeToString
      $ mconcat (fmap toCodeIndent incs)
      <> toCodeIndent r
      <> toCodeIndent mainCode

bamifyCode
  :: Int
  -> BamaFile
  -> DataFile
  -> CppSchema
  -> ([Include],Class CodeSymbol,Function CodeSymbol)
bamifyCode recNum inFile outFile schema =
  (incs
  ,recordCls "Record" schema
  ,Function
   { functionName = "main"
    ,functionType = PrimitiveType mempty CppInt
    ,functionBody = mainFn inFile outFile recNum
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
    incs =
      LibraryInclude <$> ["fcntl.h","sys/stat.h","sys/types.h","codegen_new.hh"]


mainFn :: BamaFile -> DataFile -> Int -> [Statement CodeSymbol]
mainFn inFile outFile arrLen = (ExpressionSt . Quote <$> [
  "if (argc < 2) {std::cerr << \"Too few args.\" << std::endl;}"
  , "int fd"
  , "std::array<Record," <> show arrLen <> "> recordsArr"
  , "Writer<Record> w(" <> show outFile <> ");"
  , "require_neq(fd = ::open(\"" <> inFile <> "\", O_RDONLY), -1, \"failed to open file.\")"
  ])
  ++ [
  "require_eq" .$ [readRecsCall, readSize, slit "Failed to read"]
  , writeRecs
  , mst "w" "close" []
  , "close" .$ [sym "fd"]]
  where
    readSize = ilit arrLen `expMul` sizeOfRec where
      expMul = E2ession "*"
      sizeOfRec = FunctionAp (SimpleFunctionSymbol $ Symbol "sizeof") [] ["Record"]
    readRecsCall =
      FunctionAp "::read" [] [sym "fd", m "recordsArr" "data" [], readSize]
    -- Primitives
    sym = SymbolExpression
    fn .$ args = ExpressionSt $ FunctionAp fn [] args
    mst w method args = ExpressionSt
      $ FunctionAp (InstanceMember w method) [] args
    m w method args = FunctionAp (InstanceMember w method) [] args
    writeRecs = ForEachBlock (autoDecl "r", sym "recordsArr")
      [mst "w" "write" [sym "r"]]
    autoDecl s = Declaration {
      declarationName=s,
      declarationType=autoType
      }
    autoType = ClassType mempty [] "auto"
    ilit = LiteralIntExpression
    slit = LiteralStringExpression
