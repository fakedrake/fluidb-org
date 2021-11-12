{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE TypeFamilies      #-}
module Data.Query.SQL.QFile
  (QFileConstructor(..)
  ,QFile(..)
  ,QueryFileCache(..)
  ,dataFilePath
  ,mapQFile) where

import           Data.QnfQuery.Types
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics
import           Text.Printf

newtype QFile = DataFile FilePath
  deriving (Eq,Show,Generic,Ord)

instance AShow QFile

instance ARead QFile
instance Hashable QFile
dataFilePath :: QFile -> FilePath
dataFilePath (DataFile f) = f
mapQFile :: (FilePath -> FilePath) -> QFile -> QFile
mapQFile f (DataFile x) = DataFile $ f x
-- instance IsString QFile where fromString = DataFile
class QFileConstructor constr where
  constructQFile :: Show a => constr -> a -> QFile
instance QFileConstructor (FilePath -> FilePath -> QFile) where
  constructQFile c a =
    c (printf "data%s.dat" (show a)) (printf "index%s.dat" (show a))
instance QFileConstructor (FilePath -> QFile) where
  constructQFile c a = c $ printf "data%s.dat" $ show a
data QueryFileCache e s = QueryFileCache {
  getCachedFile :: QNFQuery e s -> Maybe QFile,
  putCachedFile :: QNFQuery e s -> QFile -> QueryFileCache e s,
  delCachedFile :: QNFQuery e s -> QueryFileCache e s,
  showFileCache :: Maybe String
  }
instance Symbolic QFile where
  type SymbolType QFile = QFile
  asSymbol = Just
  mkSymbol = id
