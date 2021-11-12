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
  (mkGlobalConf
  ,mkFileCache
  ,mkQueryCppConf
  ,PreGlobalConf(..)
  ,toTableColumns
  ,CodegenSymbol(..)) where

import           Control.Monad.Writer
import           Data.Bipartite
import           Data.Cluster.InsertQuery
import           Data.Cluster.Propagators
import           Data.Cluster.Types.Clusters
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build.Types
import           Data.Codegen.SchemaAssocClass
import           Data.CppAst
import           Data.Maybe
import           Data.NodeContainers
import           Data.QnfQuery.Build
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.Optimizations.Utils
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.QFile
import           Data.Query.SQL.Types
import           Data.QueryPlan.Types
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.Unsafe
import           FluiDB.Types
import           Text.Printf

import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bifunctor
import qualified Data.HashMap.Lazy              as HM
import           Data.Query.QuerySchema
import           Data.String
import           Data.Tuple
import           Data.Utils.AShow

-- |Expr atoms that can correspond to c++ code (they have a type and a
-- name).
class CodegenSymbol e where
  codegenSymbolLiteralType :: e -> Maybe CppType
  codegenSymbolToSymbol :: IsString sym => e -> Maybe sym


-- Let's assume this is a table identifier
instance CodegenSymbol Integer where
  codegenSymbolLiteralType = const Nothing
  codegenSymbolToSymbol = Just . fromString . ("table" ++) . show
-- Let's assume this is a table identifier
instance CodegenSymbol Int where
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
    ESym x          -> Just x
    _               -> Nothing

-- | Make a QueryCppConf from the type vars
mkQueryCppConf
  :: forall e0 e s .
  (Hashables2 e s,CodegenSymbol e)
  => PreGlobalConf e0 e s
  -> QueryCppConf e s
mkQueryCppConf PreGlobalConf{..} = QueryCppConf {
  literalType = codegenSymbolLiteralType,
  tableSchema = tableSchema',
  columnType = columnTypeLocal,
  toSymbol = codegenSymbolToSymbol,
  defaultQueryFileCache = mkFileCache pgcToQFile pgcSchemaAssoc,
  uniqueColumns = (`lookup` pgcPrimKeyAssoc),
  asUnique = pgcToUniq
  }
  where
    tableSchema' :: s -> Maybe (CppSchema' e)
    tableSchema' = (`lookup` pgcSchemaAssoc)
    columnTypeLocal :: e -> s -> Maybe CppType
    columnTypeLocal e s = do
      _ <- codegenSymbolToSymbol @e @String e
      sch <- fmap2 swap $ tableSchema' s
      let ret = e `lookup` sch
      ret

mkFileCache :: forall e s .
              Hashables2 e s =>
              (s -> Maybe QFile)
            -> SchemaAssoc e s
            -> QueryFileCache e s
mkFileCache toQFile schemaAssoc =
  go $ HM.fromList $ catMaybes $ toAssoc <$> schemaAssoc
  where
    toAssoc :: (s,x) -> Maybe (Either (Query e s) (QNFQuery e s),QFile)
    toAssoc (fn,_) = (safeQnf $ Q0 fn,) <$> toQFile fn
    safeQnf :: Query e s -> Either (Query e s) (QNFQuery e s)
    safeQnf q =
      fmap (fst . fromJustErr)
      $ first (const q)
      $ (`evalStateT` def)
      $ listTMaxQNF fst
      $ toQNF (toTableColumns schemaAssoc) q
    go :: HM.HashMap (Either (Query e s) (QNFQuery e s)) QFile
       -> QueryFileCache e s
    go hm =
      QueryFileCache
      { showFileCache = Nothing  -- Just $ show hm,
       ,getCachedFile = (`HM.lookup` hm) . Right
       ,putCachedFile = \q f -> go $ HM.insert (Right q) f hm
       ,delCachedFile = \q -> go $ HM.delete (Right q) hm
      }

toTableColumns :: Eq s => SchemaAssoc e s -> s -> Maybe [e]
toTableColumns schemaAssoc = fmap2 snd . (`lookup` schemaAssoc)


-- | When calculating the global configuration for tpch we take into
-- account the follwoing:
--
-- * The tables are inserted using insertQuery
-- * As the table nodes are inserted, we are collecting the node
--   references and using tpchTableSizes
mkGlobalConf
  :: forall e0 e s t n .
  (Hashables2 e s,CodegenSymbol e,Ord s,t ~ (),n ~ ())
  => PreGlobalConf e0 e s
  -> Either (GlobalError e s t n) (GlobalConf e s t n)
mkGlobalConf pgc@PreGlobalConf {..} = do
  let gbState = def
  let (pair :: Either (ClusterError e s) (Bipartite t n,RefMap n s)
        ,clusterConfig :: ClusterConfig e s t n) =
        (`evalState` gbState)
        $ (`runStateT` def { qnfTableColumns = toTableColumns pgcSchemaAssoc })
        $ runExceptT clusterBuilt
  (propNetLocal,tableMap) <- first toGlobalError pair
  let newNodes = refKeys tableMap
  -- nodeTableSize :: NodeRef n -> Either (Maybe s) TableSize
  let nodeTableSize ref = do
        tbl <- maybe (Left Nothing) return $ ref `refLU` tableMap
        maybe (Left $ Just tbl) return $ tbl `lookup` pgcTableSizeAssoc
  nodeSizes' <- either (\x -> throwAStr $ ashow x) return
    $ traverse (\n -> (n,) . (,1) <$> nodeTableSize n)  newNodes -- (\n -> (n,) . (,1) <$> nodeTableSize n)
  return
    GlobalConf
    { globalExpTypeSymIso = pgcExpIso
     ,globalTableSizeAssoc = pgcTableSizeAssoc
     ,globalRunning = def { runningConfBudgetSearch = True }
     ,globalSchemaAssoc = pgcSchemaAssoc
     ,globalMatNodes = nNodes propNetLocal
     ,globalQueryCppConf = queryCppConf
      -- Based on this we check if they are materialized.
     ,globalClusterConfig = clusterConfig
     ,globalGCConfig = GCConfig
        { propNet = propNetLocal
         ,queryHistory = QueryHistory []
          -- historicalCosts = mempty,
          -- Note that sizes will be computed lazily.
         ,nodeSizes = refFromAssocs nodeSizes'
         ,intermediates = mempty
         ,budget = pgcBudget
         ,maxBranching = Nothing
         ,maxTreeDepth = Nothing
        }
    }
  where
    queryCppConf = mkQueryCppConf pgc
    -- Left Nothing means noderef was not found.
    clusterBuilt
      :: Monad m => CGraphBuilderT e s t n m (Bipartite t n,RefMap n s)
    clusterBuilt = do
      modify $ \cm -> cm { qnfInsertBottoms = True }
      tableMap' <- forM pgcSchemaAssoc $ \(t,_) -> do
        n <- insertQueryForest (literalType queryCppConf) . return . (t,)
          =<< symPlan t
        _shapeClust <- triggerClustPropagator $ NClustW $ NClust n
        return (n,t)
      modify $ \cm -> cm { qnfInsertBottoms = False }
      (,fromRefAssocs tableMap') <$> lift2 (gets gbPropNet)
    symPlan :: Monad m => s -> CGraphBuilderT e s t n m (QueryShape e s)
    symPlan s =
      getSymShape
        (uniqueColumns queryCppConf s)
        (tableSchema queryCppConf s)
        (s `lookup` pgcTableSizeAssoc)
        s
