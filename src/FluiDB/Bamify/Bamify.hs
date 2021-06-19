{-# LANGUAGE OverloadedStrings #-}
module FluiDB.Bamify.Bamify (mkBamaFile) where

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

import           FluiDB.Bamify.Types


-- | In-haskell parsing of the table file into a BamaFile with
-- consecutive binary POD entries.
mkBamaFile :: CppSchema -> TblFile -> BamaFile -> IO ()
mkBamaFile schema tableFile outFile = do
  fileContents <- BS.readFile tableFile
  let parser = lineParser schema
  fd <- openBinaryFile outFile WriteMode
  let eitherRes = parseLines parser tableFile fileContents
        `using` parListChunk 10 rpar
  units <- mapM (writeResultIO fd) eitherRes
  hClose fd
  putStrLn $ "Lines: " <> show (length units)



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


unsizableTypeError :: a
unsizableTypeError = error "Could not determine size of type."


writeResultIO :: Handle -> Either ParseErr BS.ByteString -> IO ()
writeResultIO fd res = case res of
  Left e        -> die $ show e
  Right binLine -> BS.hPut fd binLine


c2w :: Char -> Word8
c2w = fromIntegral . ord
w2c :: Word8 -> Char
w2c = chr . fromIntegral
bs2s :: BS.ByteString -> String
bs2s = fmap w2c . BS.unpack
nineWord :: Word8
nineWord = fromIntegral (ord '9')
zeroWord :: Word8
zeroWord = fromIntegral (ord '0')
wisDigit :: Word8 -> Bool
wisDigit x = x <= nineWord && x >= zeroWord
numStr :: MP Int
numStr = BS.foldl' (\p x -> (p * 10) + digitToInt (w2c x)) 0
         <$> takeWhileP (Just "number") wisDigit
numStr1 :: MP Int
numStr1 = BS.foldl' (\p x -> (p * 10) + digitToInt (w2c x)) 0
          <$> takeWhile1P (Just "number") wisDigit

takeUntilChar :: Char -> MP BS.ByteString
takeUntilChar c = takeWhileP (Just $ "looking for '" ++ [c] ++ "'")
                  (/= c2w c)

takeUntilOneOf :: String -> MP BS.ByteString
takeUntilOneOf s = let bs = c2w <$> s
  in
  takeWhileP (Just $ "looking for one of '" ++ s ++ "'")
  (not . (`elem` bs))
