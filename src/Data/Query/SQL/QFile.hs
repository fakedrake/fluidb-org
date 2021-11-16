{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE TypeFamilies      #-}
module Data.Query.SQL.QFile
  (QFile(..)
  ,QueryFileCache(..)
  ,dataFilePath
  ,mapQFile) where

import           Data.QnfQuery.Types
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics

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
