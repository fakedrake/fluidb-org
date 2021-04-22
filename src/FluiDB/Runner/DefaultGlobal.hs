{-# LANGUAGE CPP #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module FluiDB.Runner.DefaultGlobal
  ( DefaultGlobal(..)
  ) where

import Data.Codegen.Build.Types
import Data.Utils.Functors
import Data.CnfQuery.Build
import Data.Codegen.Schema
import Data.Utils.Default
import Data.Utils.AShow
import Data.Query.QuerySchema.SchemaBase
import Data.Bifunctor
import FluiDB.Schema.Graph.Joins
import FluiDB.Schema.Graph.Schemata
import FluiDB.Schema.Graph.Values
import Control.Monad.Except
import Control.Monad.State
import FluiDB.Schema.TPCH.Parse
import Text.Printf
import FluiDB.Schema.TPCH.Values
import FluiDB.ConfValues
import Data.Query.SQL.FileSet
import Data.Query.SQL.Types
import Data.Query.QuerySchema.Types
import Data.Query.Algebra
import FluiDB.Types
import Data.Proxy
import Data.Utils.Hashable
import FluiDB.Classes
import Data.CppAst.ExpressionLike


class (ExpressionLike e,MonadFakeIO m,Hashables2 e s) => DefaultGlobal e s t n m q | q -> e,q -> s,q -> t ,q -> n where
  defGlobalConf :: Proxy m -> [q] -> GlobalConf e s t n
  getIOQuery :: q -> GlobalSolveT e s t n m (Query (PlanSym e s) (QueryPlan e s,s))
  putPS :: Monad m =>
          Proxy q
        -> Query e s
        -> GlobalSolveT e s t n m (Query (PlanSym e s) (QueryPlan e s,s))

instance DefaultGlobal e s t n m q => DefaultGlobal e s t n m (Query e s,q) where
  defGlobalConf p = defGlobalConf p . fmap snd
  putPS _ = putPS (Proxy :: Proxy q)
  getIOQuery = putPS (Proxy :: Proxy q) . fst

-- TPC-H
instance MonadFakeIO m
  => DefaultGlobal ExpTypeSym (Maybe FileSet) () () m Int where
  defGlobalConf _ _ = tpchGlobalConf
  getIOQuery i = do
    seed <- globalPopUniqueNum
    (qtext,_) <- cmd
      "/bin/bash"
      ["-c"
      ,printf
         "DSS_QUERY=%s/queries %s/qgen -b %s/dists.dss -r %d %d | sed -e 's/where rownum <=/limit/g' -e 's/;//g'"
         qgenRoot
         qgenRoot
         qgenRoot
         (seed + 1)
         i]
    parseTpchQuery qtext
  putPS _ q = do
    cppConf <- globalQueryCppConf <$> get
    either throwError return $ putPlanSymTpch cppConf q

qgenRoot :: FilePath
#ifdef __linux__
qgenRoot = "/home/drninjabatman/Projects1/FluiDB/resources/tpch-dbgen"
#else
qgenRoot = "/Users/drninjabatman/Projects/UoE/fluidb/resources/tpch-dbgen"
#endif

-- Join-only
instance (MonadFakeIO m,Ord s,Hashable s,CodegenSymbol s,ExpressionLike (s,s))
  => DefaultGlobal (s,s) s () () m [(s,s)] where
  defGlobalConf _ = graphGlobalConf . mkGraphSchema . join
  getIOQuery js = do
    schemaAssoc <- globalSchemaAssoc <$> get
    putPS (Proxy :: Proxy [(s,s)])
      $ eqJoinQuery schemaAssoc [((s,s'),(s',s')) | (s,s') <- js]
  putPS _ q' = do
    cppConf <- globalQueryCppConf <$> get
    either (throwError . toGlobalError) (return . first (uncurry mkPlanSym))
      $ (>>= maybe (throwAStr "No cnf found") return)
      $ fmap (>>= traverse (\s -> (,s) <$> mkPlanFromTbl cppConf s) . snd)
      $ (`evalStateT` def)
      $ listTMaxCNF fst
      $ toCNF (fmap2 snd . tableSchema cppConf) q'
