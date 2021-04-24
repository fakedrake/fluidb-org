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
module Database.FluiDB.Running.DefaultGlobal
  ( DefaultGlobal(..)
  ) where

import Database.FluiDB.Codegen.Schema
import Data.Proxy
import Database.FluiDB.CppAst
import Database.FluiDB.CnfQuery
import Database.FluiDB.Default
import Database.FluiDB.AShow
import Data.Bifunctor
import Database.FluiDB.Running.Overlapping
import Database.FluiDB.Running.ParseTpch
import Control.Monad.State
import Control.Monad.Except
import Database.FluiDB.Codegen.Build.Types
import Database.FluiDB.Running.Classes
import Text.Printf
import Database.FluiDB.QuerySchema
import Database.FluiDB.Algebra
import Database.FluiDB.Utils
import Database.FluiDB.Running.ConfValues
import Database.FluiDB.Relational.GraphSchemata
import Database.FluiDB.Running.GraphValues
import Database.FluiDB.FileSet
import Database.FluiDB.Running.TpchValues
import Database.FluiDB.Relational.Parser.Types
import Database.FluiDB.Running.Types

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
instance MonadFakeIO m => DefaultGlobal ExpTypeSym (Maybe FileSet) () () m Int where
  defGlobalConf _ _ = tpchGlobalConf
  getIOQuery i = do
    seed <- globalPopUniqueNum
    (qtext,_) <- ioCmd "/bin/bash" [
      "-c",
      printf "DSS_QUERY=%s/queries %s/qgen -b %s/dists.dss -r %d %d | sed -e 's/where rownum <=/limit/g' -e 's/;//g'" qgenRoot qgenRoot qgenRoot (seed + 1) i]
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
instance (MonadFakeIO m,Ord s,Hashable s,CodegenSymbol s,ExpressionLike (s,s)) =>
         DefaultGlobal (s,s) s () () m [(s,s)] where
  defGlobalConf _ = graphGlobalConf . mkGraphSchema . join
  getIOQuery js = do
    schemaAssoc <- globalSchemaAssoc <$> get
    putPS (Proxy :: Proxy [(s,s)])
      $ eqJoinQuery schemaAssoc [((s,s'),(s',s')) | (s,s') <- js]
  putPS _ q' = do
    cppConf <- globalQueryCppConf <$> get
    either (throwError . toGlobalError)
      (return . first (uncurry mkPlanSym))
      $ (>>= maybe (throwAStr "No cnf found") return)
      $ fmap (>>= traverse (\s -> (,s) <$> mkPlanFromTbl cppConf s) . snd)
      $ (`evalStateT` def)
      $ listTMaxCNF fst
      $ toCNF (fmap2 snd . tableSchema cppConf) q'
