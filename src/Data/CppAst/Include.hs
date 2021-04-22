{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Data.CppAst.Include
  ( Include(..)
  ) where
import           Data.CppAst.LiteralCode.Codegen
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Hashable
import           GHC.Generics                    (Generic)

data Include = LocalInclude FilePath | LibraryInclude FilePath
  deriving (Eq, Show, Generic)
instance Hashable (Include)
instance AShow (Include)
instance IsCode c => CodegenIndent Include c where
  toCodeIndent' = putLine . fromString . ("#include " ++) . \case
    LocalInclude f -> "\"" ++ f ++ "\""
    LibraryInclude f -> "<" ++ f ++ ">"
