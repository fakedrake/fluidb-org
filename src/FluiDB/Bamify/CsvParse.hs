{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
module FluiDB.Bamify.CsvParse
  (bamifyFile) where

import           Control.Applicative
import           Control.Monad
import           Data.Bifunctor
import           Data.ByteString          (hGetLine)
import qualified Data.ByteString          as BS
import           Data.ByteString.Builder  as BSB
import qualified Data.ByteString.Char8    as B8
import           Data.ByteString.Internal
import           Data.Codegen.CppType
import           Data.CppAst.CppType
import           Data.List.Extra
import           Data.Query.QuerySize
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Unsafe
import           GHC.Generics
import           System.IO                hiding (hGetLine)
import           Text.Printf
import           Text.Read

type Blob = BSB.Builder
type CSVLine = BS.ByteString
type CSVField = BS.ByteString

splitColumns :: CSVLine -> Maybe [CSVField]
splitColumns l =
  case fmap (bimap BS.length reverse)
  $ uncons
  $ reverse
  $ BS.splitWith (== c2w '|') l of
    Just (0,ret) -> Just ret
    _            -> Nothing

data ParseError =
  ParseError { typeToParse :: Either [CppType] CppType,parseInput :: String }
  deriving Generic

instance AShow ParseError

parseStr :: Int -> CSVField -> Maybe Blob
parseStr size inp =
  if padding < 0 then Nothing else Just
    $ BSB.byteString inp <> mconcat (replicate padding $ int8 0)
  where
    padding = size - BS.length inp

-- Most processors are little endian
parseNat :: CSVField -> Maybe Blob
parseNat = fmap BSB.int32LE . readMaybe . B8.unpack

parseInt :: CSVField -> Maybe Blob
parseInt = fmap BSB.int32LE . readMaybe . B8.unpack

parseDouble :: CSVField -> Maybe Blob
parseDouble = fmap BSB.doubleLE . readMaybe . B8.unpack

parseChar :: CSVField -> Maybe Blob
parseChar l = if BS.length l == 1 then Just $ byteString l else Nothing

parseBool :: CSVField -> Maybe Blob
parseBool l =
  if
    | l == "0"  -> Just $ int8 0
    | l == "1"  -> Just $ int8 1
    | otherwise -> Nothing

parseDate :: CSVField -> Maybe Blob
parseDate fld = case readMaybe . B8.unpack <$> BS.splitWith (== c2w '-') fld of
  [Just year,Just month,Just day] -> Just
    $ BSB.int32LE
    $ fromInteger
    $ dateToInteger
    $ Date year month day 0 0 0
  _ -> Nothing

parseType :: CppType -> CSVField -> Either ParseError Blob
parseType ty fld = maybeToErr $ naiveParse fld
  where
    naiveParse = case ty of
      CppInt -> parseInt
      (CppArray CppChar (LiteralSize size)) -> parseStr size
      (CppArray _ _) -> const Nothing
      CppDouble -> parseDouble
      CppChar -> parseChar
      CppNat -> \fld' -> parseDate fld' <|> parseNat fld'
      CppVoid -> const Nothing
      CppBool -> parseBool
    maybeToErr Nothing =
      Left ParseError { typeToParse = Right ty,parseInput = B8.unpack fld }
    maybeToErr (Just x) = Right x



parseLine :: [CppType] -> CSVLine -> Either ParseError [Blob]
parseLine types line =
  maybe
    (Left $ ParseError { typeToParse = Left types,parseInput = B8.unpack line })
    (zipWithM parseType types)
  $ splitColumns line


-- | Pad and compine blobs
combineBlobs :: [CppType] -> [Blob] -> Maybe Blob
combineBlobs types blobs = do
  paddings <- schemaPostPaddings types
  return $ mconcat $ zipWith pad paddings blobs
  where
    pad p b = b <> mconcat (replicate p $ int8 0)

bamifyLine :: [CppType] -> CSVLine -> Either ParseError Blob
bamifyLine types line =
  maybe (Left err) Right . combineBlobs types =<< parseLine types line
  where
    err = ParseError { typeToParse = Left types,parseInput = B8.unpack line }

getFileSize :: FilePath -> IO (Maybe Integer)
getFileSize path = withFile path ReadMode $ \h -> do
  size <- hFileSize h
  return $ Just size

bamifyFile :: [CppType] -> FilePath -> FilePath -> IO TableSize
bamifyFile types tblFile bamaFile = do
  withFile tblFile ReadMode $ \rhndl -> do
    recordNum <- withFile bamaFile WriteMode $ \whndl -> readRest 0 rhndl whndl
    recordSize <- maybe (fail "Cant't get record size.") return
      $ cppSchemaSize types
    sanityCheckSize recordNum
    return
      TableSize { tableSizeRows = recordNum,tableSizeRowSize = recordSize }
  where
    sanityCheckSize recordNum = do
      actualSize <- fromJustErr <$> getFileSize bamaFile
      let expectedSize =
            toInteger $ fromJustErr (cppSchemaSize types) * recordNum
      unless (actualSize == expectedSize)
        $ fail
        $ printf
          "Expected size %d but found %d (accum size %d) for %s"
          expectedSize
          actualSize
          bamaFile
    readRest (!recNum) rhndl whndl = do
      isEof <- hIsEOF rhndl
      if isEof then return recNum else do
        line <- hGetLine rhndl
        case bamifyLine types line of
          Right struct -> do
            hPutBuilder whndl struct
            readRest (recNum + 1) rhndl whndl
          Left e -> fail $ "Parse error: " ++ ashow e
