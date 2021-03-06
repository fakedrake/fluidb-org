{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
-- |A databese with only foreign keys so we can benchmark the joins.
--
-- Implemente
-- Generate the source files.

module FluiDB.Schema.Graph.Schemata
  (graphToQFile
  ,graphTableBytes
  ,graphSchemaAssoc
  ,graphPrimKeys
  ,GraphTable(..)
  ,GraphSchema(..)
  ,GraphTypeVars
  ,mkGraphSchema
  ,graphSchemaIsoMap) where


import           Control.Monad
import           Data.Bifunctor
import           Data.Codegen.SchemaAssocClass
import           Data.CppAst
import           Data.List.Extra
import           Data.Query.QuerySize
import           Data.Query.SQL.QFile
import           Data.Utils.AShow
import           Data.Utils.Functors
import           GHC.Generics

type GraphTypeVars e s t n = (Eq s, e ~ (s,s), t ~ (), n ~ ())

-- | Since all symbols are either local or foreign keys we have an
-- identifier 's' for each table and each 'e' should be associated
-- with one or two s. We go with e ~ (s,s). Also a query is just a
-- bunch of equi-joins (ie a bunch of keys). Turn all the edges into a
-- graph.
mkGraphSchema :: forall e s t n . GraphTypeVars e s t n =>
                [(s,s)] -> GraphSchema e s
mkGraphSchema qs = GraphSchema {
  graphSchemaConnections = [(tbl, graphTable i (tbl, conns))
                           | (i,(tbl, conns)) <-
                             zip [0..] $ edgesToGraph $ nub qs],
  graphSchemaTableToQFile = const $ Just $ DataFile "no-file-really.dat"
  }
  where
    graphTable :: Int -> (s,[s]) -> GraphTable e s
    graphTable _ (tbl, ss) = GraphTable {
      graphTableForeignKeys = [(tbl', (tbl, tbl')) | tbl' <- ss],
      graphTableRows = 1000, -- 1000 + (length qs - i) * 10,
      graphTableUnique = (tbl,tbl)
      }

edgesToGraph :: Eq s => [(s,s)] -> [(s,[s])]
edgesToGraph xs = [(s,gather s) | s <- syms] where
  syms = nub $ do {(l,r) <- xs; [l,r]}
  gather s = do {(l,r) <- xs; guard $ s == l; return r}

data GraphTable e s = GraphTable {
  graphTableForeignKeys :: [(s, e)],
  graphTableRows        :: Int,
  graphTableUnique      :: e
  } deriving (Generic,Show)
instance (AShow e,AShow s) => AShow (GraphTable e s)

data GraphSchema e s = GraphSchema {
  graphSchemaConnections  :: [(s, GraphTable e s)],
  graphSchemaTableToQFile :: s -> Maybe QFile
  } deriving Generic

instance Bifunctor GraphTable where
  bimap f g GraphTable{..} = GraphTable {
    graphTableForeignKeys=fmap (g `bimap` f) graphTableForeignKeys,
    graphTableRows=graphTableRows,
    graphTableUnique=f graphTableUnique}

graphSchemaIsoMap :: (e -> e') -> (s -> s', s' -> s)
                  -> GraphSchema e s -> GraphSchema e' s'
graphSchemaIsoMap emap (f,g) GraphSchema{..} = GraphSchema{
  graphSchemaConnections=bimap f (bimap emap f) <$> graphSchemaConnections,
  graphSchemaTableToQFile=graphSchemaTableToQFile . g}

graphToQFile :: GraphSchema e s -> s -> Maybe QFile
graphToQFile = graphSchemaTableToQFile

graphTableBytes :: GraphSchema e s -> [(s, TableSize)]
graphTableBytes GraphSchema {..} =
  fmap2 (\GraphTable {..} -> TableSize
         { tsRows = graphTableRows
          ,tsRowSize = (1 + length graphTableForeignKeys) * 4
         }) graphSchemaConnections

graphSchemaAssoc :: GraphSchema e s -> SchemaAssoc e s
graphSchemaAssoc GraphSchema{..} = flip map graphSchemaConnections
  $ \(frm, GraphTable{..}) ->
      (frm, (CppNat,) <$> (graphTableUnique:(snd <$> graphTableForeignKeys)))

graphPrimKeys :: GraphSchema e s -> [(s, [e])]
graphPrimKeys gs = [(frm, [graphTableUnique])
                   | (frm, GraphTable{..}) <- graphSchemaConnections gs]
