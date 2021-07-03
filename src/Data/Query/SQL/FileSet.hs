{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE TypeFamilies      #-}
module Data.Query.SQL.FileSet
  ( FileSetConstructor(..)
  , FileSet(..)
  , QueryFileCache(..)
  , dataFilePath
  , mapFileSet
  ) where

import Data.QnfQuery.Types
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics
import           Text.Printf

data FileSet = DataFile FilePath | DataAndSet FilePath FilePath
  deriving (Eq, Show, Generic, Ord)

baseName :: FilePath -> FilePath
baseName =
  reverse . (\x -> if elem '.' x
               then tail $ dropWhile (/= '.') x
               else x) . takeWhile (/= '/') . reverse

instance AShow FileSet where
  ashow' = sexp "toFS" . \case
    DataFile f -> [Str $ baseName f]
    DataAndSet f1 f2 -> [Tup [Str $ baseName f1, Str $ baseName f2]]

instance ARead FileSet
instance Hashable FileSet
dataFilePath :: FileSet -> FilePath
dataFilePath = \case
  DataFile f -> f
  DataAndSet f _ -> f
mapFileSet :: (FilePath -> FilePath) -> FileSet -> FileSet
mapFileSet f = \case
  DataFile x -> DataFile $ f x
  DataAndSet x y -> DataAndSet (f x) (f y)
-- instance IsString FileSet where fromString = DataFile
class FileSetConstructor constr where
  constructFileSet :: Show a => constr -> a -> FileSet
instance FileSetConstructor (FilePath -> FilePath -> FileSet) where
  constructFileSet c a =
    c (printf "data%s.dat" (show a)) (printf "index%s.dat" (show a))
instance FileSetConstructor (FilePath -> FileSet) where
  constructFileSet c a = c $ printf "data%s.dat" $ show a
data QueryFileCache e s = QueryFileCache {
  getCachedFile :: QNFQuery e s -> Maybe FileSet,
  putCachedFile :: QNFQuery e s -> FileSet -> QueryFileCache e s,
  delCachedFile :: QNFQuery e s -> QueryFileCache e s,
  showFileCache :: Maybe String
  }
instance Symbolic FileSet where
  type SymbolType FileSet = FileSet
  asSymbol = Just
  mkSymbol = id
