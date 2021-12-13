{-# LANGUAGE CPP                    #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeSynonymInstances   #-}
{-# LANGUAGE UndecidableInstances   #-}
module FluiDB.Runner.DefaultGlobal
  ( DefaultGlobal(..)
  ) where

import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bifunctor
import           Data.Codegen.Build.Types
import           Data.Codegen.Schema
import           Data.CppAst.ExpressionLike
import           Data.Proxy
import           Data.QnfQuery.Build
import           Data.Query.Algebra
import           Data.Query.QuerySchema.SchemaBase
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.QFile
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           FluiDB.Classes
import           FluiDB.ConfValues
import           FluiDB.Schema.Common
import           FluiDB.Schema.Graph.Joins
import           FluiDB.Schema.Graph.Schemata
import           FluiDB.Schema.Graph.Values
import           FluiDB.Schema.TPCH.Parse
import           FluiDB.Schema.TPCH.Values
import           FluiDB.Types
import           Text.Printf

-- XXX: deprecate this in favor of workload params
class (ExpressionLike e,MonadFakeIO m,Hashables2 e s)
  => DefaultGlobal e s t n m q | q -> e,q -> s,q -> t,q -> n where
  defGlobalConf :: Proxy m -> [q] -> GlobalConf e s t n
  getIOQuery
    :: q -> GlobalSolveT e s t n m (Query (ShapeSym e s) (QueryShape e s,s))
  putPS :: Monad m
        => Proxy q
        -> Query e s
        -> GlobalSolveT e s t n m (Query (ShapeSym e s) (QueryShape e s,s))

instance DefaultGlobal e s t n m q => DefaultGlobal e s t n m (Query e s,q) where
  defGlobalConf p = defGlobalConf p . fmap snd
  putPS _ = putPS (Proxy :: Proxy q)
  getIOQuery = putPS (Proxy :: Proxy q) . fst

-- TPC-H
instance MonadFakeIO m
  => DefaultGlobal ExpTypeSym (Maybe QFile) () () m Int where
  defGlobalConf _ _ = tpchGlobalConf
  getIOQuery i = do
    seed <- globalPopUniqueNum
    (qtext,_) <- ioCmd
      ioOps
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
    cppConf <- gets globalQueryCppConf
    symSizeAssoc <- gets globalTableSizeAssoc
    let luSize s = lookup s symSizeAssoc
    either throwError return $ annotateQuery cppConf luSize q

qgenRoot :: FilePath
qgenRoot = "/Users/drninjabatman/Projects/UoE/fluidb/resources/tpch-dbgen"

-- Join-only
instance (MonadFakeIO m
         ,Ord s
         ,Hashable s
         ,CodegenSymbol s
         ,ExpressionLike (s,s)
         ,AShowV s) => DefaultGlobal (s,s) s () () m [(s,s)] where
  defGlobalConf _ x = graphGlobalConf $ mkGraphSchema $ join x
  getIOQuery js = do
    schemaAssoc <- gets globalSchemaAssoc
    putPS (Proxy :: Proxy [(s,s)])
      $ eqJoinQuery schemaAssoc [((s,s'),(s',s')) | (s,s') <- js]
  putPS _ q' = do
    cppConf <- gets globalQueryCppConf
    symSizeAssoc <- gets globalTableSizeAssoc
    let luSize s = lookup s symSizeAssoc
    let mkShape s = do
          size <- luSize s
          mkShapeFromTbl cppConf size s
    either (throwError . toGlobalError) (return . first (uncurry mkShapeSym))
      $ (>>= maybe (throwAStr "No qnf found") return)
      $ fmap (>>= traverse (\s -> (,s) <$> mkShape s) . snd)
      $ (`evalStateT` def)
      $ listTMaxQNF fst
      $ toQNF (fmap2 snd . tableSchema cppConf) q'
