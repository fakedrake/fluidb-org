{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

-- | Build the values
module FluiDB.ConfValues
  ( mkGlobalConf
  , mkFileCache
  , mkQueryCppConf
  , toTableColumns
  , CodegenSymbol(..)
  ) where

import Data.Query.QuerySchema.GetQueryPlan
import Data.Cluster.Types.Clusters
import Data.Cluster.Propagators
import Data.CnfQuery.Build
import Data.Utils.Unsafe
import Data.Query.Optimizations.Utils
import Text.Printf
import Data.Cluster.InsertQuery
import Data.Utils.Default
import Data.Utils.Functors
import Data.QueryPlan.Types
import Data.NodeContainers
import Data.BipartiteGraph
import Data.Cluster.Types.Monad
import FluiDB.Types
import Data.Query.QuerySize
import Data.CnfQuery.Types
import Data.Query.Algebra
import Data.Codegen.SchemaAssocClass
import Data.Query.QuerySchema.Types
import Data.Codegen.Build.Types
import Data.Query.SQL.FileSet
import Data.Utils.Hashable
import Data.Query.SQL.Types
import Data.CppAst
import           Control.Monad.Writer
import           Data.Maybe

import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bifunctor
import qualified Data.HashMap.Lazy                        as HM
import           Data.String
import           Data.Tuple

-- |Expr atoms that can correspond to c++ code (they have a type and a
-- name).
class CodegenSymbol e where
  codegenSymbolLiteralType :: e -> Maybe CppType
  codegenSymbolToSymbol :: IsString sym => e -> Maybe sym


-- Let's assume this is a table identifier
instance CodegenSymbol Integer where
  codegenSymbolLiteralType = const Nothing
  codegenSymbolToSymbol = Just . fromString . ("table" ++) . show
instance CodegenSymbol s => CodegenSymbol (s,s) where
  codegenSymbolLiteralType = const Nothing
  codegenSymbolToSymbol :: forall sym . (CodegenSymbol s, IsString sym) =>
                          (s,s) -> Maybe sym
  codegenSymbolToSymbol (x,y) =
    fmap fromString $ printf "from_%s_to_%s" <$> go x <*> go y
    where
      go :: s -> Maybe String
      go = codegenSymbolToSymbol

instance CodegenSymbol ExpTypeSym where
  codegenSymbolLiteralType = expTypeSymCppType
  codegenSymbolToSymbol = fmap fromString . \case
    ECount (Just x) -> Just x
    ESym x -> Just x
    _ -> Nothing

-- | Make a QueryCppConf from the type vars
mkQueryCppConf :: forall e s .
                 (Hashables2 e s, CodegenSymbol e) =>
                 (Int -> e -> Maybe e)
               -> (s -> Maybe FileSet)
               -> [(s, [e])]
               -> SchemaAssoc e s
               -> QueryCppConf e s
mkQueryCppConf asUniq relFileSet primKeyAssoc schemaAssoc = QueryCppConf {
  literalType = codegenSymbolLiteralType,
  tableSchema = tableSchema',
  columnType = columnTypeLocal,
  toSymbol = codegenSymbolToSymbol,
  defaultQueryFileCache = mkFileCache relFileSet schemaAssoc,
  uniqueColumns = (`lookup` primKeyAssoc),
  asUnique = asUniq
  }
  where
    tableSchema' :: s -> Maybe (CppSchema' e)
    tableSchema' = (`lookup` schemaAssoc)
    columnTypeLocal :: e -> s -> Maybe CppType
    columnTypeLocal e s = do
      _ <- codegenSymbolToSymbol @e @String e
      sch <- fmap2 swap $ tableSchema' s
      let ret = e `lookup` sch
      ret

mkFileCache :: forall e s .
              Hashables2 e s =>
              (s -> Maybe FileSet)
            -> SchemaAssoc e s
            -> QueryFileCache e s
mkFileCache toFileSet schemaAssoc = go
  $ HM.fromList
  $ catMaybes
  $ toAssoc <$> schemaAssoc
  where
    toAssoc :: (s, x) -> Maybe (Either (Query e s) (CNFQuery e s), FileSet)
    toAssoc (fn, _) = (safeCnf $ Q0 fn,) <$> toFileSet fn
    safeCnf :: Query e s -> Either (Query e s) (CNFQuery e s)
    safeCnf q = fmap (fst . fromJustErr)
                $ first (const q)
                $ (`evalStateT` def)
                $ listTMaxCNF fst
                $ toCNF (toTableColumns schemaAssoc) q
    go :: HM.HashMap (Either (Query e s) (CNFQuery e s)) FileSet -> QueryFileCache e s
    go hm = QueryFileCache{
      showFileCache = Nothing, -- Just $ show hm,
      getCachedFile = (`HM.lookup` hm) . Right,
      putCachedFile = \q f -> go $ HM.insert (Right q) f hm,
      delCachedFile = \q -> go $ HM.delete (Right q) hm
      }

toTableColumns :: Eq s => SchemaAssoc e s -> s -> Maybe [e]
toTableColumns schemaAssoc = fmap2 snd . (`lookup` schemaAssoc)

-- | When calculating the global configuration for tpch we take into
-- account the follwoing:
--
-- * The tables are inserted using insertQuery
-- * As the table nodes are inserted, we are collecting the node
--   references and using tpchTableSizes
mkGlobalConf :: forall e e0 s t n .
               (Hashables2 e s,
                CodegenSymbol e, Ord s,
                t ~ (), n ~ ()) =>
               (e -> ExpTypeSym' e0,ExpTypeSym' e0 -> e)
             -> (Int -> e -> Maybe e)
             -> (s -> Maybe FileSet) -- Embedding of tables in filesets
             -> [(s,[e])]          -- Primary keys of each table
             -> SchemaAssoc e s     -- The schema of each table
             -> [(s,TableSize)]          -- Size of each table in bytes
             -> Maybe (GlobalConf e s t n)
mkGlobalConf expIso toUniq toFileSet primKeyAssoc schemaAssoc tableSizeAssoc = do
  let gbState = mempty
  let (pair :: Either (ClusterError e s) (Bipartite t n,RefMap n s)
        ,clusterConfig :: ClusterConfig e s t n) =
        (`evalState` gbState)
        $ (`runStateT` def { cnfTableColumns = toTableColumns schemaAssoc })
        $ runExceptT clusterBuilt
  (propNetLocal,tableMap) <- either (const Nothing) Just pair
  let newNodes = refKeys tableMap
  -- nodeTableSize :: NodeRef n -> Either (Maybe s) [TableSize]
  let nodeTableSize ref = do
        tbl <- maybe (Left Nothing) return $ ref `refLU` tableMap
        maybe (Left $ Just tbl) return2 $ tbl `lookup` tableSizeAssoc
  nodeSizes' <- either (const Nothing) Just
    $ traverse (\n -> (n,) . (,1) <$> nodeTableSize n)
    $ newNodes
  return
    GlobalConf
    { globalExpTypeSymIso = expIso
     ,globalRunning = def { runningConfBudgetSearch = True }
     ,globalSchemaAssoc = schemaAssoc
     ,globalMatNodes = refKeys $ rNodes propNetLocal
     ,globalQueryCppConf = queryCppConf
      -- Based on this we check if they are materialized.
     ,globalClusterConfig = clusterConfig
     ,globalGCConfig = GCConfig
        { propNet = propNetLocal
          -- historicalCosts = mempty,
          -- Note that sizes will be computed lazily.
         ,nodeSizes = refFromAssocs nodeSizes'
         ,intermediates = mempty
         ,budget = Nothing
         ,maxBranching = Nothing
         ,maxTreeDepth = Nothing
        }
    }
  where
    queryCppConf = mkQueryCppConf toUniq toFileSet primKeyAssoc schemaAssoc
    -- Left Nothing means noderef was not found.
    clusterBuilt
      :: Monad m => CGraphBuilderT e s t n m (Bipartite t n,RefMap n s)
    clusterBuilt = do
      modify $ \cm -> cm { cnfInsertBottoms = True }
      tableMap' <- forM schemaAssoc $ \(t,_) -> do
        n <- insertQueryPlan (literalType queryCppConf)
          =<< return . (t,) <$> symPlan t
        triggerClustPropagator $ NClustW $ NClust n
        return (n,t)
      modify $ \cm -> cm { cnfInsertBottoms = False }
      (,fromRefAssocs tableMap') <$> lift2 (gbPropNet <$> get)
    symPlan :: Monad m => s -> CGraphBuilderT e s t n m (QueryPlan e s)
    symPlan =
      getSymPlan (uniqueColumns queryCppConf) (tableSchema queryCppConf)
