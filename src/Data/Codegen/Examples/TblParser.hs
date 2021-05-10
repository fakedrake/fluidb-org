{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}

module Data.Codegen.Examples.TblParser
  ( parseLines
  ) where

import Data.Query.QuerySchema.Types
import qualified Data.ByteString.Lazy              as BS
import qualified Data.ByteString.Builder      as BSB
import qualified Data.ByteString.Lazy.Char8        as BSC
import           GHC.Int
import           GHC.Word

import           Data.Char
import           Data.Maybe
import           Data.Void
import           Data.CppAst            as CC
import           Data.Query.SQL.Types
import           Text.Megaparsec
import           Text.Megaparsec.Byte

cppTypeSize :: CppType -> Maybe Int
cppTypeSize = \case
  CppArray t (LiteralSize l) -> (* l) <$> cppTypeSize t
  CppArray _ (SizeOf _) -> Nothing
  CppVoid -> Nothing
  CppChar -> Just 1
  CppNat -> Just 4
  CppInt -> Just 4
  CppDouble -> Just 8
  CppBool -> Just 1

cppTypeAlignment :: CppType -> Maybe Int
cppTypeAlignment = \case
  CppArray t _ -> cppTypeSize t
  x -> cppTypeSize x

schemaPostPaddings :: [CC.CppType] -> Maybe [Int]
schemaPostPaddings [] = Just []
schemaPostPaddings [_] = Just [0]
schemaPostPaddings schema = do
    elemSizes <- sequenceA [cppTypeSize t | t <- schema]
    spaceAligns' <- sequenceA [cppTypeAlignment t | t <- schema]
    let (_:spaceAligns) = spaceAligns' ++ [maximum spaceAligns']
    let offsets = 0:zipWith3 getOffset' spaceAligns offsets elemSizes
    return $ zipWith (-) (zipWith (-) (tail offsets) offsets) elemSizes
    where
      getOffset' nextAlig off size = (size + off)
                                    + ((nextAlig -
                                        ((size + off) `mod` nextAlig))
                                       `mod` nextAlig)

c2w :: Char -> Word8
c2w = fromIntegral . ord
w2c :: Word8 -> Char
w2c = chr . fromIntegral
bs2s :: BS.ByteString -> String
bs2s = fmap w2c . BS.unpack

type MP a = Parsec Void BS.ByteString a
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

unsizableTypeError :: a
unsizableTypeError = error "Could not determine size of type."

lineParser :: CppSchema -> MP BS.ByteString
lineParser [] = eof >> return mempty
lineParser fields = do
  r <- foldl1 sequenceParsers $ zipWith toPaddedParser prePads fields
  _ <- readBar
  _ <- eof
  return r
  where
    prePads = sizedNullStr <$> fromMaybe unsizableTypeError
              (schemaPostPaddings $ fmap fst fields)
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

parseLines' :: MP BS.ByteString
           -> String
           -> BS.ByteString
           -> Maybe Int -- Limit
           -> [Either (ParseErrorBundle BSC.ByteString Void) BSC.ByteString]
parseLines' parser tableFile fileContents linesM =
  parse' <$> takeM linesM  (BSC.lines fileContents)
  where
    parse' = parse parser tableFile
    takeM :: Maybe Int -> [a] -> [a]
    takeM Nothing  = id
    takeM (Just i) = take i

parseLines :: CppSchema
           -> String
           -> BS.ByteString
           -> Maybe Int -- Limit
           -> [Either (ParseErrorBundle BSC.ByteString Void) BSC.ByteString]
parseLines = parseLines' . lineParser
