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

module Database.FluiDB.Running.GraphValues
  ( graphGlobalConf
  , graphRelationColumns
  ) where

import Control.Monad
import Database.FluiDB.Relational.Parser.Types
import           Data.Hashable
import           Database.FluiDB.Relational.GraphSchemata
import           Database.FluiDB.Running.ConfValues
import           Database.FluiDB.Running.Types

-- | graphGlobalConf $ mkGraphSchema [[(1,2)]]
graphGlobalConf :: forall e s t n .
                  (CodegenSymbol s, Ord s, Hashable s,
                   GraphTypeVars e s t n) =>
                  GraphSchema e s -> GlobalConf e s t n
graphGlobalConf sch = fromJust' $ mkGlobalConf
  (ESym,\case {ESym a -> a;e -> error $ "Graph has only syms: " ++ show (void e)})
  (\_ _ -> Nothing)
  (graphToFileSet sch)
  (graphPrimKeys sch)
  (graphSchemaAssoc sch)
  (graphTableBytes sch)
  where
    fromJust' = \case
      Nothing -> error "Couldn't make a GlobalConf"
      Just x -> x

graphRelationColumns :: GraphTypeVars e s t n => GraphSchema e s -> s -> Maybe [e]
graphRelationColumns GraphSchema{..} sym = do
  GraphTable{..} <- sym `lookup` graphSchemaConnections
  return $ graphTableUnique:(snd <$> graphTableForeignKeys)
