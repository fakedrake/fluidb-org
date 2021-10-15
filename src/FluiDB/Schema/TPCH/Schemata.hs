{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

module FluiDB.Schema.TPCH.Schemata
  (customer
  ,lineitem
  ,nation
  ,orders
  ,part
  ,partsupp
  ,region
  ,supplier
  ,tpchSchemaAssoc
  ,tpchTablePrimKeys
  ,Table(..)
  ,tpchTableSizes) where

import           Control.Monad
import           Data.Codegen.SchemaAssocClass
import           Data.CppAst
import           Data.Query.QuerySize
import           Data.Query.SQL.Types
import           Data.String
import           Data.Utils.Functors
import           Data.Utils.Unsafe

type GenSchema = forall a x . IsString a => [(CppTypeF x, a)]

tpchTablePrimKeys :: IsString a => [(Table, [a])]
tpchTablePrimKeys = [
  ("customer",["c_custkey"]),
  ("lineitem",["l_orderkey", "l_linenumber"]),
  ("nation",["n_nationkey"]),
  ("orders",["o_orderkey"]),
  ("part",["p_partkey"]),
  ("partsupp",["ps_suppkey","ps_partkey"]),
  ("region",["r_regionkey"]),
  ("supplier",["s_suppkey"])]


tpchTableSizes :: [(Table,TableSize)]
tpchTableSizes =
  fmap check
  $ fromJustErr
  $ toTableSizes
    tpchSchemaAssoc
    [("part",34136064)
    ,("customer",33336)
    ,("lineitem",857324)
    ,("partsupp",182046720)
    ,("region",4096)
    ,("supplier",2158592)
    ,("orders",214292)
    ,("nation",8192)]
  where
    check p@(_,TableSize {tsRowSize = r}) = if r <= 0 then error "yes" else p

toTableSizes :: Ord s => SchemaAssoc e s -> [(s,Int)] -> Maybe [(s,TableSize)]
toTableSizes schemaAssoc tableBytes' = do
  rs <- traverse2 schemaSize schemaAssoc
  forM_ rs (\(_,x) -> when (x < 0) $ error "Ooops")
  return
    [(tbl,tableSize' 4096 thisTableBytes rowSize)
    | (tbl,(rowSize,thisTableBytes)) <- alignMaps rs tableBytes']
  where
    alignMaps l r = [(k,(v,fromJustErr $ lookup k r)) | (k,v) <- l]

customer :: GenSchema
customer =
  [(CppNat,"c_custkey")
  ,(strType 18,"c_name")
  ,(strType 40,"c_address")
  ,(CppNat,"c_nationkey")
  ,(strType 15,"c_phone")
  ,(CppDouble,"c_acctbal")
  ,(strType 10,"c_mktsegment")
  ,(strType 117,"c_comment")]

lineitem :: GenSchema
lineitem =
  [(CppNat,"l_orderkey")
  ,(CppNat,"l_partkey")
  ,(CppNat,"l_suppkey")
  ,(CppInt,"l_linenumber")
  ,(CppNat,"l_quantity")
  ,(CppDouble,"l_extendedprice")
  ,(CppDouble,"l_discount")
  ,(CppDouble,"l_tax")
  ,(strType 1,"l_returnflag")
  ,(strType 1,"l_linestatus")
  ,(dateType,"l_shipdate")
  ,(dateType,"l_commitdate")
  ,(dateType,"l_receiptdate")
  ,(strType 25,"l_shipinstruct")
  ,(strType 10,"l_shipmode")
  ,(strType 44,"l_comment")]

nation :: GenSchema
nation =
  [(CppNat,"n_nationkey")
  ,(strType 25,"n_name")
  ,(CppNat,"n_regionkey")
  ,(strType 152,"n_comment")]

orders :: GenSchema
orders =
  [(CppNat,"o_orderkey")
  ,(CppNat,"o_custkey")
  ,(strType 1,"o_orderstatus")
  ,(CppDouble,"o_totalprice")
  ,(dateType,"o_orderdate")
  ,(strType 15,"o_orderpriority")
  ,(strType 15,"o_clerk")
  ,(CppInt,"o_shippriority")
  ,(strType 79,"o_comment")]

part :: GenSchema
part =
  [(CppNat,"p_partkey")
  ,(strType 55,"p_name")
  ,(strType 25,"p_mfgr")
  ,(strType 10,"p_brand")
  ,(strType 25,"p_type")
  ,(CppInt,"p_size")
  ,(strType 10,"p_container")
  ,(CppDouble,"p_retailprice")
  ,(strType 23,"p_comment")]

partsupp :: GenSchema
partsupp =
  [(CppNat,"ps_partkey")
  ,(CppNat,"ps_suppkey")
  ,(CppInt,"ps_availqty")
  ,(CppDouble,"ps_supplycost")
  ,(strType 199,"ps_comment")]

region :: GenSchema
region =
  [(CppNat,"r_regionkey"),(strType 25,"r_name"),(strType 152,"r_comment")]

supplier :: GenSchema
supplier =
  [(CppNat,"s_suppkey")
  ,(strType 25,"s_name")
  ,(strType 40,"s_address")
  ,(CppNat,"s_nationkey")
  ,(strType 15,"s_phone")
  ,(CppDouble,"s_acctbal")
  ,(strType 101,"s_comment")]


tpchSchemaAssoc :: forall e s . (IsString e,IsString s) => SchemaAssoc e s
tpchSchemaAssoc = [
  ("customer",customer),
  ("lineitem",lineitem),
  ("nation",nation),
  ("orders",orders),
  ("part",part),
  ("partsupp",partsupp),
  ("region",region),
  ("supplier",supplier)
  ]

strType :: Int -> CppTypeF a
strType = CppArray CppChar . LiteralSize

dateType :: CppTypeF a
dateType = CppNat
