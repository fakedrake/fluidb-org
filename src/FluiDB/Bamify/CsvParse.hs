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
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           GHC.Generics
import           System.IO                hiding (hGetLine)
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

bamifyFile :: [CppType] -> FilePath -> FilePath -> IO ()
bamifyFile types tblFile bamaFile = do
  withFile tblFile ReadMode
    $ \rhndl -> withFile bamaFile WriteMode $ \whndl -> readRest rhndl whndl
  where
    readRest rhndl whndl = do
      isEof <- hIsEOF rhndl
      when isEof $ do
        line <- hGetLine rhndl
        case bamifyLine types line of
          Right struct -> hPutBuilder whndl struct >> readRest rhndl whndl
          Left e       -> fail $ "Parse error: " ++ ashow e
