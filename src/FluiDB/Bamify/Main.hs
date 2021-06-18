{-# LANGUAGE CPP                 #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}

module FluiDB.Bamify.Main
  (mkAllDataFiles
  ,DBGenConf(..)
  ,DBGenTableConf(..)) where

import           Control.Monad
import           Control.Parallel.Strategies
import qualified Data.ByteString.Builder               as BSB
import qualified Data.ByteString.Lazy                  as BS
import qualified Data.ByteString.Lazy.Char8            as BSC
import           Data.Char
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
import           Data.Maybe
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.Types
import qualified Data.Set                              as DS
import           Data.Void
import           FluiDB.Schema.TPCH.Schemata
import           GHC.Int
import           GHC.Word
import           System.Directory
import           System.Exit
import           System.FilePath.Posix
import           System.IO
import           System.Posix.Files
import           Text.Megaparsec
import           Text.Megaparsec.Byte
import           Text.Printf




mainFn :: BamaFile -> DataFile -> Int -> [Statement CodeSymbol]
mainFn inFile outFile arrLen = (ExpressionSt . Quote <$> [
  "if (argc < 2) {std::cerr << \"Too few args.\" << std::endl;}"
  , "int fd"
  , "std::array<Record," <> show arrLen <> "> recordsArr"
  , "Writer<Record> w(" <> outFile <> ");"
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


takeUntilChar :: Char -> MP BS.ByteString
takeUntilChar c = takeWhileP (Just $ "looking for '" ++ [c] ++ "'")
                  (/= c2w c)

takeUntilOneOf :: String -> MP BS.ByteString
takeUntilOneOf s = let bs = c2w <$> s
  in
  takeWhileP (Just $ "looking for one of '" ++ s ++ "'")
  (not . (`elem` bs))

unsizableTypeError :: a
unsizableTypeError = error "Could not determine size of type."

-- | Space separated hex representations of the bytes.
showSerial :: BS.ByteString -> String
showSerial = (printf "%02x " . ord . w2c) <=< BS.unpack

lineParser :: CppSchema -> MP BS.ByteString
lineParser [] = eof >> return mempty
lineParser fields = do
  r <- foldl1 sequenceParsers $ zipWith toPaddedParser prePads fields
  _ <- readBar
  _ <- eof
  return r
  where
    prePads = sizedNullStr <$> fromMaybe unsizableTypeError
              (schemaPostPaddings fields)
      where
        sizedNullStr x = BS.take (fromIntegral x) $ BS.repeat 0
    toPaddedParser p t = (`mappend` p) <$> toParser t
    readBar = tokens (==) "|"
    sequenceParsers :: MP BS.ByteString -> MP BS.ByteString -> MP BS.ByteString
    sequenceParsers parsers b = do
      p1 <- parsers
      _ <- readBar
      p2 <- b
      return $ p1 `mappend` p2
    toParser (t, _) = case t of
      CppArray CppChar (LiteralSize l) ->
        BS.take (fromIntegral l) . (<> BS.repeat 0) <$> takeUntilChar '|'
      CppArray _ _ -> undefined
      CppDouble -> BSB.toLazyByteString . BSB.doubleLE <$> do
        sign <- option 1.0 $ tokens (==) "-" >> return (-1.0)
        intPrt <- numStr
        decPrt <- option 0 $ do
          _ <- char $ c2w '.'
          ret <- takeWhile1P (Just "decimal part") wisDigit
          return $ BS.foldr
            (\x p -> p * 0.1 + fromIntegral (digitToInt $ w2c x)) 0 ret
        let ret = sign * (fromIntegral intPrt + 0.1 * decPrt)
        return ret
      CppChar -> BS.singleton <$> anySingle
      CppNat -> BSB.toLazyByteString . BSB.int32LE
        <$> (try parseDate <|> fmap fromIntegral parseNat) where
        parseDate :: MP Int32
        parseDate = do
          y <- getNat 4
          _ <- char $ c2w '-'
          m <- getNat 2
          _ <- char $ c2w '-'
          d <- getNat 2
          return $ fromInteger $ dateToInteger $ Date y m d 0 0 0
        getNat :: Int -> MP Integer
        getNat i = BS.foldl (\x y ->
                               x*10 + fromIntegral y - fromIntegral (ord '0')) 0
                   <$> takeP (Just "digits") i
        parseNat = try numStr1 <|> (
          do
            throwStr <- takeUntilOneOf "#|"
            stopper <- lookAhead $ takeP (Just "Field stop char.") 1
            if stopper == "#"
              then numStr1
              else fail $ bs2s
                   $ "Expected a number, got: \"" <> throwStr <> "\"")
      CppVoid -> undefined
      CppBool -> (char (c2w '1') >> return "\1") <|> (char (c2w '0') >> return "\0")
      CppInt -> BSB.toLazyByteString . BSB.int32LE . fromIntegral <$>
        ((*) <$> option 1 (tokens (==) "-" >> return (-1)) <*> numStr1)

writeResultIO :: Handle -> Either ParseErr BS.ByteString -> IO ()
writeResultIO fd res = case res of
  Left e        -> die $ show e
  Right binLine -> BS.hPut fd binLine

type CppCode = String


parseLines :: MP BS.ByteString
           -> String
           -> BS.ByteString
           -> [Either ParseErr BSC.ByteString]
parseLines parser tableFile fileContents = parse'
  <$> zip [1::Int ..] (BSC.lines fileContents)
  where
    parse' (linum, line) = parse (parser' linum) tableFile line
    parser' _linum = do
      -- setPosition $ (initialPos tableFile){sourceLine=mkPos linum}
      parser

-- | In-haskell parsing of the table file into a BamaFile with
-- consecutive binary POD entries.
mkBamaFile :: TblFile -> BamaFile -> IO ()
mkBamaFile tableFile outFile = do
  fileContents <- BS.readFile tableFile
  schema <- getSchemaOrDie $ takeBaseName tableFile
  let parser = lineParser schema
  fd <- openBinaryFile outFile WriteMode
  let eitherRes = parseLines parser tableFile fileContents
        `using` parListChunk 10 rpar
  units <- mapM (writeResultIO fd) eitherRes
  hClose fd
  putStrLn $ "Lines: " <> show (length units)

-- | Runs dbgen to build all in the current directory.
mkAllDataFiles :: DBGenConf -> IO ()
mkAllDataFiles DBGenConf{..} = do
  curDir <- getCurrentDirectory
  let checkExists = dbGenConfIncremental
  forM_ dbGenConfTables $ \DBGenTableConf{..} -> do
    let datFile = curDir </> dbGenTableConfFileBase <.> "dat"
    withExist checkExists datFile $ do
      runProc $ mkProc dbGenConfExec ["-s",show dbGenConfScale,"-T",[dbGenTableConfChar]]
      let tblFile = curDir </> dbGenTableConfFileBase <.> "tbl"
      mkDataFile tblFile datFile
  where
    withExists False _ m = m
    withExists True datFile m = do
      exists <- doesFileExist datFile
      if exists then return () else m
