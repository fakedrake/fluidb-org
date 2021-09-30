{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Query.QuerySize
  (pageNum
  ,PageNum
  ,TableSize(..)
  ,cppTypeSize
  ,schemaSize
  ,tableSize'
  ,PageSize
  ,Cardinality
  ,Bytes
  ,tableBytes
  ,schemaSizeModify
  ,tableSizeModify
  ,tableSizeComb) where

import           Data.CppAst.CppType
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Hashable
import           GHC.Generics

type Bytes = Int
type Cardinality = Int
data TableSize = TableSize { tsRows :: Cardinality,tsRowSize :: Bytes }
  deriving (Eq,Show,Read,Generic)
instance Ord TableSize where
  compare a b = compare (tableBytes a) (tableBytes b)
tableBytes :: TableSize -> Bytes
tableBytes TableSize{..} = tsRows * tsRowSize
instance AShow TableSize
instance Hashable TableSize
instance ARead TableSize
instance Default TableSize

type PageNum = Int
type PageSize = Bytes
-- | Returns nothing if the row is larger than the page size.
pageNum :: PageSize -> TableSize -> Maybe PageNum
pageNum pageSize TableSize {..} =
  if tsRowSize > pageSize then Nothing else Just ret
  where
    ret =
      if rowsPerPage == 0
      then 1 else max 1 $ tsRows `div` rowsPerPage + finalPage
    rowsPerPage =
      if tsRowSize == 0 then tsRows -- all rows are here
      else pageSize `div` tsRowSize
    finalPage
      | tsRowSize == 0 = 0
      | pageSize `mod` tsRowSize == 0 = 0
      | otherwise = 1

cppTypeSize :: CppTypeF a -> Maybe Bytes
cppTypeSize = \case
  CppArray t (LiteralSize l) -> (* l) <$> cppTypeSize t
  CppArray _ (SizeOf _)      -> Nothing
  CppVoid                    -> Nothing
  CppChar                    -> Just 1
  CppNat                     -> Just 4
  CppInt                     -> Just 4
  CppDouble                  -> Just 8
  CppBool                    -> Just 1

tableSize' :: PageSize -> Bytes -> Bytes -> TableSize
tableSize' pageSize totalSize rowSize =
  if rowSize < 0 then error "oops"
  else TableSize { tsRows = recsInPage * totalPages,tsRowSize = rowSize }
  where
    recsInPage =
      if rowSize == 0
      then error "We need nonzero row size" else pageSize `div` rowSize
    totalPages =
      if pageSize == 0 || recsInPage == 0 then 1 else totalSize
        `divCeil` (recsInPage * pageSize)
      where
        divCeil x y = (x `div` y) + (if x `mod` y == 0 then 0 else 1)

cppTypeAlignment :: CppTypeF a -> Maybe Int
cppTypeAlignment = \case
  CppArray t _ -> cppTypeSize t
  x            -> cppTypeSize x

schemaSize :: [(CppTypeF x, a)] -> Maybe Bytes
schemaSize [] = Just 0
schemaSize [(x, _)] = cppTypeSize x
schemaSize schema = do
  elemSizes <- sequenceA [cppTypeSize t | (t,_) <- schema]
  spaceAlignsIsolated <- sequenceA [cppTypeAlignment t | (t,_) <- schema]
  let (_:spaceAligns) = spaceAlignsIsolated ++ [maximum spaceAlignsIsolated]
  let offsets = 0 : zipWith3 getOffset spaceAligns offsets elemSizes
  return $ last offsets + last elemSizes
  where
    getOffset nextAlig off size =
      (size + off)
      + ((nextAlig - ((size + off) `mod` nextAlig)) `mod` nextAlig)

tableSizeComb :: (Int -> Int -> (Int,Double)) -> (Bytes -> Bytes -> (Bytes,Double))
              -> TableSize -> TableSize -> (TableSize,Double)
tableSizeComb combWidth combHeight ts1 ts2 =
  (TableSize { tsRows = height,tsRowSize = width },wCert * hCert)
  where
    (width,wCert) = tsRowSize ts1 `combWidth` tsRowSize ts2
    (height,hCert) = tsRows ts1 `combHeight` tsRows ts2
tableSizeModify :: (Double -> Double) -> (Double -> Double) -> TableSize -> TableSize
tableSizeModify f g TableSize {..} =
  TableSize
  { tsRows = round $ f $ fromIntegral tsRows
   ,tsRowSize = round $ g $ fromIntegral tsRowSize
  }
schemaSizeModify :: (Int -> Int) -> TableSize -> TableSize
schemaSizeModify f ts = ts { tsRowSize = f $ tsRowSize ts }
