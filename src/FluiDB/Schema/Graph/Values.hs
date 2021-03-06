{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

module FluiDB.Schema.Graph.Values
  ( graphGlobalConf
  , graphRelationColumns
  ) where

import           Control.Monad
import           Data.Hashable
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Unsafe
import           FluiDB.ConfValues
import           FluiDB.Schema.Graph.Schemata
import           FluiDB.Types

-- | graphGlobalConf $ mkGraphSchema [[(1,2)]]
graphGlobalConf
  :: forall e s t n .
  (CodegenSymbol s,Ord s,Hashable s,GraphTypeVars e s t n,AShowV s,AShowV e)
  => GraphSchema e s
  -> GlobalConf e s t n
graphGlobalConf sch =
  fromRightErr
  $ mkGlobalConf
  $ PreGlobalConf
  { pgcExpIso = (ESym,\case
      ESym a -> a
      e      -> error $ "Graph has only syms: " ++ show (void e))
   ,pgcToUniq = \_ _ -> Nothing
   ,pgcToQFile = graphToQFile sch
   ,pgcPrimKeyAssoc = graphPrimKeys sch
   ,pgcSchemaAssoc = graphSchemaAssoc sch
   ,pgcTableSizeAssoc = graphTableBytes sch
   ,pgcBudget = Nothing
   ,pgcDataDir = "/tmp/fluidb_data"
  }

graphRelationColumns :: GraphTypeVars e s t n => GraphSchema e s -> s -> Maybe [e]
graphRelationColumns GraphSchema{..} sym = do
  GraphTable{..} <- sym `lookup` graphSchemaConnections
  return $ graphTableUnique:(snd <$> graphTableForeignKeys)
