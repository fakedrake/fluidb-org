{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
module Data.Codegen.Optimization (planSymKeysOnlySymEmbedding) where

import Data.QnfQuery.Types
import Data.Maybe
import Data.CppAst.CppType
import Data.Query.QuerySchema.SchemaBase
import Data.Query.QuerySchema.Types
import Data.Codegen.Build.Types
import Data.Query.Optimizations.Types
import Data.Utils.Hashable

-- A sym embedding for a key-only schema (the graph schema in particular)
planSymKeysOnlySymEmbedding
  :: Hashables2 e s => QueryCppConf e s -> SymEmbedding e s (PlanSym e s)
planSymKeysOnlySymEmbedding QueryCppConf {..} =
  SymEmbedding
  { embedLit = mkLitPlanSym
   ,unEmbed = planSymOrig
   ,symEq = (==)
   ,embedType = maybe (Just CppNat) Just . literalType . planSymOrig
    -- ^ everything is a key here
   ,embedInS = \e s -> isJust $ columnType (planSymOrig e) s
   ,embedIsLit = \sym -> case planSymQnfName sym of
      NonSymbolName _ -> False
      _ -> True
  }
