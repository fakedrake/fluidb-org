{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
module Data.Codegen.Optimization (shapeSymKeysOnlySymEmbedding) where

import Data.QnfQuery.Types
import Data.Maybe
import Data.CppAst.CppType
import Data.Query.QuerySchema.SchemaBase
import Data.Query.QuerySchema.Types
import Data.Codegen.Build.Types
import Data.Query.Optimizations.Types
import Data.Utils.Hashable

-- A sym embedding for a key-only schema (the graph schema in particular)
shapeSymKeysOnlySymEmbedding
  :: Hashables2 e s => QueryCppConf e s -> SymEmbedding e s (ShapeSym e s)
shapeSymKeysOnlySymEmbedding QueryCppConf {..} =
  SymEmbedding
  { embedLit = mkLitShapeSym
   ,unEmbed = shapeSymOrig
   ,symEq = (==)
   ,embedType = maybe (Just CppNat) Just . literalType . shapeSymOrig
    -- ^ everything is a key here
   ,embedInS = \e s -> isJust $ columnType (shapeSymOrig e) s
   ,embedIsLit = \sym -> case shapeSymQnfName sym of
      NonSymbolName _ -> False
      _ -> True
  }
