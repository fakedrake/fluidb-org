{-# LANGUAGE OverloadedStrings #-}
module Data.Codegen.Examples.ParsedTpch
  ( tpchQueryMap
  ) where

import           Data.String
import           Data.Query.Algebra
import           Data.Query.SQL.Types

tpchQueryMap :: IsString s => [(String, Query ExpTypeSym (Maybe s))]
tpchQueryMap = [("/home/drninjabatman/Projects/FluiDB/resources/tpch_query1.sql",
  (Q1
    (QSort [(E0 (ESym "l_returnflag")),(E0 (ESym "l_linestatus"))])
    (Q1
      (QGroup
        [((ESym "l_returnflag"),
          (E0 (NAggr AggrFirst (E0 (ESym "l_returnflag"))))),
         ((ESym "l_linestatus"),
          (E0 (NAggr AggrFirst (E0 (ESym "l_linestatus"))))),
         ((ESym "sum_qty"),
          (E0 (NAggr AggrSum (E0 (ESym "l_quantity"))))),
         ((ESym "sum_base_price"),
          (E0 (NAggr AggrSum (E0 (ESym "l_extendedprice"))))),
         ((ESym "sum_disc_price"),
          (E0
            (NAggr
              AggrSum
              (E2 EMul
                  (E0 (ESym "l_extendedprice"))
                  (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount"))))))),
         ((ESym "sum_charge"),
          (E0
            (NAggr
              AggrSum
              (E2
                EMul
                (E0 (ESym "l_extendedprice"))
                (E2 EMul
                    (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount")))
                    (E2 EAdd (E0 (EInt 1)) (E0 (ESym "l_tax")))))))),
         ((ESym "avg_qty"),
          (E0 (NAggr AggrAvg (E0 (ESym "l_quantity"))))),
         ((ESym "avg_price"),
          (E0 (NAggr AggrAvg (E0 (ESym "l_extendedprice"))))),
         ((ESym "avg_disc"),
          (E0 (NAggr AggrAvg (E0 (ESym "l_discount"))))),
         ((ESym "count_order"),
          (E0 (NAggr AggrFirst (E0 (ECount Nothing)))))]
        [(E0 (ESym "l_returnflag")),(E0 (ESym "l_linestatus"))])
      (S
        (P0
          (R2
            RLe
            (R0 (E0 (ESym "l_shipdate")))
            (R0
              (E2
                ESub
                (E0
                  (EDate (Date {year = 1998, month = 12, day = 1, hour = 0, minute = 0, seconds = 0})))
                (E0
                  (EInterval (Date {year = 0, month = 0, day = 90, hour = 0, minute = 0, seconds = 0})))))))
        (Q0
          (Just
            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat")))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query2.sql",
  (Q1
    (QSort
      [(E1 ENeg (E0 (ESym "s_acctbal"))),
       (E0 (ESym "n_name")),
       (E0 (ESym "s_name")),
       (E0 (ESym "p_partkey"))])
    (Q1
      (QProj
        [((ESym "s_acctbal"),(E0 (ESym "s_acctbal"))),
         ((ESym "s_name"),(E0 (ESym "s_name"))),
         ((ESym "n_name"),(E0 (ESym "n_name"))),
         ((ESym "p_partkey"),(E0 (ESym "p_partkey"))),
         ((ESym "p_mfgr"),(E0 (ESym "p_mfgr"))),
         ((ESym "s_address"),(E0 (ESym "s_address"))),
         ((ESym "s_phone"),(E0 (ESym "s_phone"))),
         ((ESym "s_comment"),(E0 (ESym "s_comment")))])
      (Q2
        QProjQuery
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_partkey")))
                  (R0 (E0 (ESym "ps_partkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "s_suppkey")))
                    (R0 (E0 (ESym "ps_suppkey")))))
              (And
                (P0 (R2 REq (R0 (E0 (ESym "p_size"))) (R0 (E0 (EInt 15)))))
                (And
                  (P0
                    (R2 RLike
                        (R0 (E0 (ESym "p_type")))
                        (R0 (E0 (EString "%BRASS")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "s_nationkey")))
                          (R0 (E0 (ESym "n_nationkey")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "n_regionkey")))
                            (R0 (E0 (ESym "r_regionkey")))))
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "r_name")))
                            (R0 (E0 (EString "EUROPE")))))))))))
          (Q2
            QProd
            (Q2
              QProd
              (Q2
                QProd
                (Q2
                  QProd
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/region.dat"))))
        (Q2
          QDistinct
          (S
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_partkey")))
                    (R0 (E0 (ESym "ps_partkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "s_suppkey")))
                      (R0 (E0 (ESym "ps_suppkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "p_size")))
                        (R0 (E0 (EInt 15)))))
                  (And
                    (P0
                      (R2 RLike
                          (R0 (E0 (ESym "p_type")))
                          (R0 (E0 (EString "%BRASS")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "s_nationkey")))
                            (R0 (E0 (ESym "n_nationkey")))))
                      (And
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "n_regionkey")))
                              (R0 (E0 (ESym "r_regionkey")))))
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "r_name")))
                              (R0 (E0 (EString "EUROPE")))))))))))
            (Q2
              QProd
              (Q2
                QProd
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/region.dat"))))
          (J
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "ps_supplycost")))
                    (R0 (E0 (ESym "unnested_query0")))))
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_partkey")))
                    (R0 (E0 (ESym "ps.ps_partkey"))))))
            (S
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "p_partkey")))
                      (R0 (E0 (ESym "ps_partkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "s_suppkey")))
                        (R0 (E0 (ESym "ps_suppkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "p_size")))
                          (R0 (E0 (EInt 15)))))
                    (And
                      (P0
                        (R2 RLike
                            (R0 (E0 (ESym "p_type")))
                            (R0 (E0 (EString "%BRASS")))))
                      (And
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "s_nationkey")))
                              (R0 (E0 (ESym "n_nationkey")))))
                        (And
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "n_regionkey")))
                                (R0 (E0 (ESym "r_regionkey")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "r_name")))
                                (R0 (E0 (EString "EUROPE")))))))))))
              (Q2
                QProd
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q2
                      QProd
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/region.dat"))))
            (Q1
              (QGroup [] [(E0 (ESym "p_partkey"))])
              (S
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "s.s_suppkey")))
                        (R0 (E0 (ESym "ps.ps_suppkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "s.s_nationkey")))
                          (R0 (E0 (ESym "n.n_nationkey")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "n.n_regionkey")))
                            (R0 (E0 (ESym "r.r_regionkey")))))
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "r.r_name")))
                            (R0 (E0 (EString "EUROPE"))))))))
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q2
                      QProd
                      (Q1
                        (QProj
                          [((ESym "ps.ps_partkey"),
                            (E0 (ESym "ps_partkey"))),
                           ((ESym "ps.ps_suppkey"),
                            (E0 (ESym "ps_suppkey"))),
                           ((ESym "ps.ps_supplycost"),
                            (E0 (ESym "ps_supplycost")))])
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                      (Q1
                        (QProj
                          [((ESym "s.s_suppkey"),
                            (E0 (ESym "s_suppkey"))),
                           ((ESym "s.s_nationkey"),
                            (E0 (ESym "s_nationkey")))])
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))))
                    (Q1
                      (QProj
                        [((ESym "n.n_nationkey"),
                          (E0 (ESym "n_nationkey"))),
                         ((ESym "n.n_regionkey"),
                          (E0 (ESym "n_regionkey")))])
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
                  (Q1
                    (QProj
                      [((ESym "r.r_regionkey"),
                        (E0 (ESym "r_regionkey"))),
                       ((ESym "r.r_name"),(E0 (ESym "r_name")))])
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/region.dat")))))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query3.sql",
  (Q1
    (QLimit 10)
    (Q1
      (QSort [(E1 ENeg (E0 (ESym "revenue"))),(E0 (ESym "o_orderdate"))])
      (Q1
        (QGroup
          [((ESym "l_orderkey"),
            (E0 (NAggr AggrFirst (E0 (ESym "l_orderkey"))))),
           ((ESym "revenue"),
            (E0
              (NAggr
                AggrSum
                (E2 EMul
                    (E0 (ESym "l_extendedprice"))
                    (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount"))))))),
           ((ESym "o_orderdate"),
            (E0 (NAggr AggrFirst (E0 (ESym "o_orderdate"))))),
           ((ESym "o_shippriority"),
            (E0 (NAggr AggrFirst (E0 (ESym "o_shippriority")))))]
          [(E0 (ESym "l_orderkey")),
           (E0 (ESym "o_orderdate")),
           (E0 (ESym "o_shippriority"))])
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "c_mktsegment")))
                  (R0 (E0 (EString "BUILDING")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "c_custkey")))
                    (R0 (E0 (ESym "o_custkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "l_orderkey")))
                      (R0 (E0 (ESym "o_orderkey")))))
                (And
                  (P0
                    (R2
                      RLt
                      (R0 (E0 (ESym "o_orderdate")))
                      (R0
                        (E0
                          (EDate (Date {year = 1995, month = 3, day = 15, hour = 0, minute = 0, seconds = 0}))))))
                  (P0
                    (R2
                      RGt
                      (R0 (E0 (ESym "l_shipdate")))
                      (R0
                        (E0
                          (EDate (Date {year = 1995, month = 3, day = 15, hour = 0, minute = 0, seconds = 0}))))))))))
          (Q2
            QProd
            (Q2
              QProd
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat"))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat")))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query4.sql",
  (Q1
    (QSort [(E0 (ESym "o_orderpriority"))])
    (Q1
      (QGroup
        [((ESym "o_orderpriority"),
          (E0 (NAggr AggrFirst (E0 (ESym "o_orderpriority"))))),
         ((ESym "order_count"),
          (E0 (NAggr AggrFirst (E0 (ECount Nothing)))))]
        [(E0 (ESym "o_orderpriority"))])
      (Q2
        QProjQuery
        (S
          (And
            (P0
              (R2
                RGe
                (R0 (E0 (ESym "o_orderdate")))
                (R0
                  (E0
                    (EDate (Date {year = 1993, month = 7, day = 1, hour = 0, minute = 0, seconds = 0}))))))
            (P0
              (R2
                RLt
                (R0 (E0 (ESym "o_orderdate")))
                (R0
                  (E2
                    EAdd
                    (E0
                      (EDate (Date {year = 1993, month = 7, day = 1, hour = 0, minute = 0, seconds = 0})))
                    (E0
                      (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
        (Q2
          QDistinct
          (S
            (And
              (P0
                (R2
                  RGe
                  (R0 (E0 (ESym "o_orderdate")))
                  (R0
                    (E0
                      (EDate (Date {year = 1993, month = 7, day = 1, hour = 0, minute = 0, seconds = 0}))))))
              (P0
                (R2
                  RLt
                  (R0 (E0 (ESym "o_orderdate")))
                  (R0
                    (E2
                      EAdd
                      (E0
                        (EDate (Date {year = 1993, month = 7, day = 1, hour = 0, minute = 0, seconds = 0})))
                      (E0
                        (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
          (J
            (P0
              (R2 REq
                  (R0 (E0 (ESym "l.l_orderkey")))
                  (R0 (E0 (ESym "o_orderkey")))))
            (S
              (And
                (P0
                  (R2
                    RGe
                    (R0 (E0 (ESym "o_orderdate")))
                    (R0
                      (E0
                        (EDate (Date {year = 1993, month = 7, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                (P0
                  (R2
                    RLt
                    (R0 (E0 (ESym "o_orderdate")))
                    (R0
                      (E2
                        EAdd
                        (E0
                          (EDate (Date {year = 1993, month = 7, day = 1, hour = 0, minute = 0, seconds = 0})))
                        (E0
                          (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
            (S
              (P0
                (R2 RLt
                    (R0 (E0 (ESym "l.l_commitdate")))
                    (R0 (E0 (ESym "l.l_receiptdate")))))
              (Q1
                (QProj
                  [((ESym "l.l_orderkey"),(E0 (ESym "l_orderkey"))),
                   ((ESym "l.l_commitdate"),(E0 (ESym "l_commitdate"))),
                   ((ESym "l.l_receiptdate"),
                    (E0 (ESym "l_receiptdate")))])
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat")))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query5.sql",
  (Q1
    (QSort [(E1 ENeg (E0 (ESym "revenue")))])
    (Q1
      (QGroup
        [((ESym "n_name"),(E0 (NAggr AggrFirst (E0 (ESym "n_name"))))),
         ((ESym "revenue"),
          (E0
            (NAggr
              AggrSum
              (E2 EMul
                  (E0 (ESym "l_extendedprice"))
                  (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount")))))))]
        [(E0 (ESym "n_name"))])
      (S
        (And
          (P0
            (R2 REq
                (R0 (E0 (ESym "c_custkey")))
                (R0 (E0 (ESym "o_custkey")))))
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "l_orderkey")))
                  (R0 (E0 (ESym "o_orderkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "l_suppkey")))
                    (R0 (E0 (ESym "s_suppkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "c_nationkey")))
                      (R0 (E0 (ESym "s_nationkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "s_nationkey")))
                        (R0 (E0 (ESym "n_nationkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "n_regionkey")))
                          (R0 (E0 (ESym "r_regionkey")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "r_name")))
                            (R0 (E0 (EString "ASIA")))))
                      (And
                        (P0
                          (R2
                            RGe
                            (R0 (E0 (ESym "o_orderdate")))
                            (R0
                              (E0
                                (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                        (P0
                          (R2
                            RLt
                            (R0 (E0 (ESym "o_orderdate")))
                            (R0
                              (E2
                                EAdd
                                (E0
                                  (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                                (E0
                                  (EInterval (Date {year = 1, month = 0, day = 0, hour = 0, minute = 0, seconds = 0})))))))))))))))
        (Q2
          QProd
          (Q2
            QProd
            (Q2
              QProd
              (Q2
                QProd
                (Q2
                  QProd
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/region.dat"))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query6.sql",
  (Q1
    (QGroup
      [((ESym "revenue"),
        (E0
          (NAggr
            AggrSum
            (E2 EMul
                (E0 (ESym "l_extendedprice"))
                (E0 (ESym "l_discount"))))))]
      [])
    (S
      (And
        (P0
          (R2
            RGe
            (R0 (E0 (ESym "l_shipdate")))
            (R0
              (E0
                (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
        (And
          (P0
            (R2
              RLt
              (R0 (E0 (ESym "l_shipdate")))
              (R0
                (E2
                  EAdd
                  (E0
                    (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                  (E0
                    (EInterval (Date {year = 1, month = 0, day = 0, hour = 0, minute = 0, seconds = 0})))))))
          (And
            (And
              (P0
                (R2
                  RLe
                  (R0 (E2 ESub (E0 (EFloat 6.0e-2)) (E0 (EFloat 1.0e-2))))
                  (R0 (E0 (ESym "l_discount")))))
              (P0
                (R2
                  RLe
                  (R0 (E0 (ESym "l_discount")))
                  (R0 (E2 EAdd (E0 (EFloat 6.0e-2)) (E0 (EFloat 1.0e-2)))))))
            (P0 (R2 RLt (R0 (E0 (ESym "l_quantity"))) (R0 (E0 (EInt 24))))))))
      (Q0
        (Just
          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query7.sql",
  (Q1
    (QSort
      [(E0 (ESym "supp_nation")),
       (E0 (ESym "cust_nation")),
       (E0 (ESym "l_year"))])
    (Q1
      (QGroup
        [((ESym "supp_nation"),
          (E0 (NAggr AggrFirst (E0 (ESym "supp_nation"))))),
         ((ESym "cust_nation"),
          (E0 (NAggr AggrFirst (E0 (ESym "cust_nation"))))),
         ((ESym "l_year"),(E0 (NAggr AggrFirst (E0 (ESym "l_year"))))),
         ((ESym "revenue"),(E0 (NAggr AggrSum (E0 (ESym "volume")))))]
        [(E0 (ESym "supp_nation")),
         (E0 (ESym "cust_nation")),
         (E0 (ESym "l_year"))])
      (Q1
        (QProj
          [((ESym "supp_nation"),(E0 (ESym "n1.n_name"))),
           ((ESym "cust_nation"),(E0 (ESym "n2.n_name"))),
           ((ESym "l_year"),
            (E1 (EFun ExtractYear) (E0 (ESym "l_shipdate")))),
           ((ESym "volume"),
            (E2 EMul
                (E0 (ESym "l_extendedprice"))
                (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount")))))])
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "s_suppkey")))
                  (R0 (E0 (ESym "l_suppkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "o_orderkey")))
                    (R0 (E0 (ESym "l_orderkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "c_custkey")))
                      (R0 (E0 (ESym "o_custkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "s_nationkey")))
                        (R0 (E0 (ESym "n1.n_nationkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "c_nationkey")))
                          (R0 (E0 (ESym "n2.n_nationkey")))))
                    (And
                      (Or
                        (And
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "n1.n_name")))
                                (R0 (E0 (EString "FRANCE")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "n2.n_name")))
                                (R0 (E0 (EString "GERMANY"))))))
                        (And
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "n1.n_name")))
                                (R0 (E0 (EString "GERMANY")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "n2.n_name")))
                                (R0 (E0 (EString "FRANCE")))))))
                      (And
                        (P0
                          (R2
                            RLe
                            (R0
                              (E0
                                (EDate (Date {year = 1995, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))
                            (R0 (E0 (ESym "l_shipdate")))))
                        (P0
                          (R2
                            RLe
                            (R0 (E0 (ESym "l_shipdate")))
                            (R0
                              (E0
                                (EDate (Date {year = 1996, month = 12, day = 31, hour = 0, minute = 0, seconds = 0})))))))))))))
          (Q2
            QProd
            (Q2
              QProd
              (Q2
                QProd
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat")))
              (Q1
                (QProj
                  [((ESym "n1.n_nationkey"),(E0 (ESym "n_nationkey"))),
                   ((ESym "n1.n_name"),(E0 (ESym "n_name")))])
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
            (Q1
              (QProj
                [((ESym "n2.n_nationkey"),(E0 (ESym "n_nationkey"))),
                 ((ESym "n2.n_name"),(E0 (ESym "n_name")))])
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query8.sql",
  (Q1
    (QSort [(E0 (ESym "o_year"))])
    (Q1
      (QGroup
        [((ESym "o_year"),(E0 (NAggr AggrFirst (E0 (ESym "o_year"))))),
         ((ESym "mkt_share"),
          (E2
            EDiv
            (E0
              (NAggr
                AggrSum
                (E2
                  EOr
                  (E2
                    EAnd
                    (E2 EEq
                        (E0 (ESym "nation"))
                        (E0 (EString "BRAZIL")))
                    (E0 (ESym "volume")))
                  (E0 (EInt 0)))))
            (E0 (NAggr AggrSum (E0 (ESym "volume"))))))]
        [(E0 (ESym "o_year"))])
      (Q1
        (QProj
          [((ESym "o_year"),
            (E1 (EFun ExtractYear) (E0 (ESym "o_orderdate")))),
           ((ESym "volume"),
            (E2 EMul
                (E0 (ESym "l_extendedprice"))
                (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount"))))),
           ((ESym "nation"),(E0 (ESym "n2.n_name")))])
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_partkey")))
                  (R0 (E0 (ESym "l_partkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "s_suppkey")))
                    (R0 (E0 (ESym "l_suppkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "l_orderkey")))
                      (R0 (E0 (ESym "o_orderkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "o_custkey")))
                        (R0 (E0 (ESym "c_custkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "c_nationkey")))
                          (R0 (E0 (ESym "n1.n_nationkey")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "n1.n_regionkey")))
                            (R0 (E0 (ESym "r_regionkey")))))
                      (And
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "r_name")))
                              (R0 (E0 (EString "AMERICA")))))
                        (And
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "s_nationkey")))
                                (R0 (E0 (ESym "n2.n_nationkey")))))
                          (And
                            (And
                              (P0
                                (R2
                                  RLe
                                  (R0
                                    (E0
                                      (EDate (Date {year = 1995, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))
                                  (R0 (E0 (ESym "o_orderdate")))))
                              (P0
                                (R2
                                  RLe
                                  (R0 (E0 (ESym "o_orderdate")))
                                  (R0
                                    (E0
                                      (EDate (Date {year = 1996, month = 12, day = 31, hour = 0, minute = 0, seconds = 0})))))))
                            (P0
                              (R2
                                REq
                                (R0 (E0 (ESym "p_type")))
                                (R0
                                  (E0 (EString "ECONOMY ANODIZED STEEL"))))))))))))))
          (Q2
            QProd
            (Q2
              QProd
              (Q2
                QProd
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q2
                      QProd
                      (Q2
                        QProd
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat")))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat")))
                (Q1
                  (QProj
                    [((ESym "n1.n_nationkey"),
                      (E0 (ESym "n_nationkey"))),
                     ((ESym "n1.n_regionkey"),
                      (E0 (ESym "n_regionkey")))])
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
              (Q1
                (QProj
                  [((ESym "n2.n_nationkey"),(E0 (ESym "n_nationkey"))),
                   ((ESym "n2.n_name"),(E0 (ESym "n_name")))])
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/region.dat")))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query9.sql",
  (Q1
    (QSort [(E0 (ESym "nation")),(E1 ENeg (E0 (ESym "o_year")))])
    (Q1
      (QGroup
        [((ESym "nation"),(E0 (NAggr AggrFirst (E0 (ESym "nation"))))),
         ((ESym "o_year"),(E0 (NAggr AggrFirst (E0 (ESym "o_year"))))),
         ((ESym "sum_profit"),
          (E0 (NAggr AggrSum (E0 (ESym "amount")))))]
        [(E0 (ESym "nation")),(E0 (ESym "o_year"))])
      (Q1
        (QProj
          [((ESym "nation"),(E0 (ESym "n_name"))),
           ((ESym "o_year"),
            (E1 (EFun ExtractYear) (E0 (ESym "o_orderdate")))),
           ((ESym "amount"),
            (E2
              ESub
              (E2 EMul
                  (E0 (ESym "l_extendedprice"))
                  (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount"))))
              (E2 EMul
                  (E0 (ESym "ps_supplycost"))
                  (E0 (ESym "l_quantity")))))])
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "s_suppkey")))
                  (R0 (E0 (ESym "l_suppkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "ps_suppkey")))
                    (R0 (E0 (ESym "l_suppkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "ps_partkey")))
                      (R0 (E0 (ESym "l_partkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "p_partkey")))
                        (R0 (E0 (ESym "l_partkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "o_orderkey")))
                          (R0 (E0 (ESym "l_orderkey")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "s_nationkey")))
                            (R0 (E0 (ESym "n_nationkey")))))
                      (P0
                        (R2 RLike
                            (R0 (E0 (ESym "p_name")))
                            (R0 (E0 (EString "%green%")))))))))))
          (Q2
            QProd
            (Q2
              QProd
              (Q2
                QProd
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query10.sql",
  (Q1
    (QLimit 20)
    (Q1
      (QSort [(E1 ENeg (E0 (ESym "revenue")))])
      (Q1
        (QGroup
          [((ESym "c_custkey"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_custkey"))))),
           ((ESym "c_name"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_name"))))),
           ((ESym "revenue"),
            (E0
              (NAggr
                AggrSum
                (E2 EMul
                    (E0 (ESym "l_extendedprice"))
                    (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount"))))))),
           ((ESym "c_acctbal"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_acctbal"))))),
           ((ESym "n_name"),
            (E0 (NAggr AggrFirst (E0 (ESym "n_name"))))),
           ((ESym "c_address"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_address"))))),
           ((ESym "c_phone"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_phone"))))),
           ((ESym "c_comment"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_comment")))))]
          [(E0 (ESym "c_custkey")),
           (E0 (ESym "c_name")),
           (E0 (ESym "c_acctbal")),
           (E0 (ESym "c_phone")),
           (E0 (ESym "n_name")),
           (E0 (ESym "c_address")),
           (E0 (ESym "c_comment"))])
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "c_custkey")))
                  (R0 (E0 (ESym "o_custkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "l_orderkey")))
                    (R0 (E0 (ESym "o_orderkey")))))
              (And
                (P0
                  (R2
                    RGe
                    (R0 (E0 (ESym "o_orderdate")))
                    (R0
                      (E0
                        (EDate (Date {year = 1993, month = 10, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                (And
                  (P0
                    (R2
                      RLt
                      (R0 (E0 (ESym "o_orderdate")))
                      (R0
                        (E2
                          EAdd
                          (E0
                            (EDate (Date {year = 1993, month = 10, day = 1, hour = 0, minute = 0, seconds = 0})))
                          (E0
                            (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0})))))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "l_returnflag")))
                          (R0 (E0 (EString "R")))))
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "c_nationkey")))
                          (R0 (E0 (ESym "n_nationkey"))))))))))
          (Q2
            QProd
            (Q2
              QProd
              (Q2
                QProd
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat"))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat")))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query11.sql",
  (Q1
    (QSort [(E1 ENeg (E0 (ESym "value")))])
    (Q1
      (QProj
        [((ESym "ps_partkey"),(E0 (ESym "ps_partkey"))),
         ((ESym "value"),(E0 (ESym "value")))])
      (Q2
        QProjQuery
        (Q1
          (QGroup
            [((ESym "ps_partkey"),
              (E0 (NAggr AggrFirst (E0 (ESym "ps_partkey"))))),
             ((ESym "value"),
              (E0
                (NAggr
                  AggrSum
                  (E2 EMul
                      (E0 (ESym "ps_supplycost"))
                      (E0 (ESym "ps_availqty")))))),
             ((ESym "having_aggregation12"),
              (E0
                (NAggr
                  AggrSum
                  (E2 EMul
                      (E0 (ESym "ps_supplycost"))
                      (E0 (ESym "ps_availqty"))))))]
            [(E0 (ESym "ps_partkey"))])
          (S
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "ps_suppkey")))
                    (R0 (E0 (ESym "s_suppkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "s_nationkey")))
                      (R0 (E0 (ESym "n_nationkey")))))
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "n_name")))
                      (R0 (E0 (EString "GERMANY")))))))
            (Q2
              QProd
              (Q2
                QProd
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat"))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))))
        (Q2
          QDistinct
          (Q1
            (QGroup
              [((ESym "ps_partkey"),
                (E0 (NAggr AggrFirst (E0 (ESym "ps_partkey"))))),
               ((ESym "value"),
                (E0
                  (NAggr
                    AggrSum
                    (E2 EMul
                        (E0 (ESym "ps_supplycost"))
                        (E0 (ESym "ps_availqty")))))),
               ((ESym "having_aggregation12"),
                (E0
                  (NAggr
                    AggrSum
                    (E2 EMul
                        (E0 (ESym "ps_supplycost"))
                        (E0 (ESym "ps_availqty"))))))]
              [(E0 (ESym "ps_partkey"))])
            (S
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "ps_suppkey")))
                      (R0 (E0 (ESym "s_suppkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "s_nationkey")))
                        (R0 (E0 (ESym "n_nationkey")))))
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "n_name")))
                        (R0 (E0 (EString "GERMANY")))))))
              (Q2
                QProd
                (Q2
                  QProd
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat"))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))))
          (J
            (P0
              (R2 RGt
                  (R0 (E0 (ESym "having_aggregation12")))
                  (R0 (E0 (ESym "unnested_query0")))))
            (Q1
              (QGroup
                [((ESym "ps_partkey"),
                  (E0 (NAggr AggrFirst (E0 (ESym "ps_partkey"))))),
                 ((ESym "value"),
                  (E0
                    (NAggr
                      AggrSum
                      (E2 EMul
                          (E0 (ESym "ps_supplycost"))
                          (E0 (ESym "ps_availqty")))))),
                 ((ESym "having_aggregation12"),
                  (E0
                    (NAggr
                      AggrSum
                      (E2 EMul
                          (E0 (ESym "ps_supplycost"))
                          (E0 (ESym "ps_availqty"))))))]
                [(E0 (ESym "ps_partkey"))])
              (S
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "ps_suppkey")))
                        (R0 (E0 (ESym "s_suppkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "s_nationkey")))
                          (R0 (E0 (ESym "n_nationkey")))))
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "n_name")))
                          (R0 (E0 (EString "GERMANY")))))))
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat"))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat")))))
            (Q1
              (QGroup [] [])
              (S
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "ps_suppkey")))
                        (R0 (E0 (ESym "s_suppkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "s_nationkey")))
                          (R0 (E0 (ESym "n_nationkey")))))
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "n_name")))
                          (R0 (E0 (EString "GERMANY")))))))
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat"))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query12.sql",
  (Q1
    (QSort [(E0 (ESym "l_shipmode"))])
    (Q1
      (QGroup
        [((ESym "l_shipmode"),
          (E0 (NAggr AggrFirst (E0 (ESym "l_shipmode"))))),
         ((ESym "high_line_count"),
          (E0
            (NAggr
              AggrSum
              (E2
                EOr
                (E2
                  EAnd
                  (E2
                    EOr
                    (E2 EEq
                        (E0 (ESym "o_orderpriority"))
                        (E0 (EString "1-URGENT")))
                    (E2 EEq
                        (E0 (ESym "o_orderpriority"))
                        (E0 (EString "2-HIGH"))))
                  (E0 (EInt 1)))
                (E0 (EInt 0)))))),
         ((ESym "low_line_count"),
          (E0
            (NAggr
              AggrSum
              (E2
                EOr
                (E2
                  EAnd
                  (E2
                    EAnd
                    (E2 ENEq
                        (E0 (ESym "o_orderpriority"))
                        (E0 (EString "1-URGENT")))
                    (E2 ENEq
                        (E0 (ESym "o_orderpriority"))
                        (E0 (EString "2-HIGH"))))
                  (E0 (EInt 1)))
                (E0 (EInt 0))))))]
        [(E0 (ESym "l_shipmode"))])
      (S
        (And
          (P0
            (R2 REq
                (R0 (E0 (ESym "o_orderkey")))
                (R0 (E0 (ESym "l_orderkey")))))
          (And
            (Or
              (P0
                (R2 REq
                    (R0 (E0 (ESym "l_shipmode")))
                    (R0 (E0 (EString "MAIL")))))
              (P0
                (R2 REq
                    (R0 (E0 (ESym "l_shipmode")))
                    (R0 (E0 (EString "SHIP"))))))
            (And
              (P0
                (R2 RLt
                    (R0 (E0 (ESym "l_commitdate")))
                    (R0 (E0 (ESym "l_receiptdate")))))
              (And
                (P0
                  (R2 RLt
                      (R0 (E0 (ESym "l_shipdate")))
                      (R0 (E0 (ESym "l_commitdate")))))
                (And
                  (P0
                    (R2
                      RGe
                      (R0 (E0 (ESym "l_receiptdate")))
                      (R0
                        (E0
                          (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                  (P0
                    (R2
                      RLt
                      (R0 (E0 (ESym "l_receiptdate")))
                      (R0
                        (E2
                          EAdd
                          (E0
                            (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                          (E0
                            (EInterval (Date {year = 1, month = 0, day = 0, hour = 0, minute = 0, seconds = 0}))))))))))))
        (Q2
          QProd
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat"))
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query14.sql",
  (Q1
    (QGroup
      [((ESym "promo_revenue"),
        (E2
          EMul
          (E0 (NAggr AggrFirst (E0 (EFloat 100.0))))
          (E2
            EDiv
            (E0
              (NAggr
                AggrSum
                (E2
                  EOr
                  (E2
                    EAnd
                    (E2 ELike
                        (E0 (ESym "p_type"))
                        (E0 (EString "PROMO%")))
                    (E2 EMul
                        (E0 (ESym "l_extendedprice"))
                        (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount")))))
                  (E0 (EInt 0)))))
            (E0
              (NAggr
                AggrSum
                (E2 EMul
                    (E0 (ESym "l_extendedprice"))
                    (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount")))))))))]
      [])
    (S
      (And
        (P0
          (R2 REq
              (R0 (E0 (ESym "l_partkey")))
              (R0 (E0 (ESym "p_partkey")))))
        (And
          (P0
            (R2
              RGe
              (R0 (E0 (ESym "l_shipdate")))
              (R0
                (E0
                  (EDate (Date {year = 1995, month = 9, day = 1, hour = 0, minute = 0, seconds = 0}))))))
          (P0
            (R2
              RLt
              (R0 (E0 (ESym "l_shipdate")))
              (R0
                (E2
                  EAdd
                  (E0
                    (EDate (Date {year = 1995, month = 9, day = 1, hour = 0, minute = 0, seconds = 0})))
                  (E0
                    (EInterval (Date {year = 0, month = 1, day = 0, hour = 0, minute = 0, seconds = 0})))))))))
      (Q2
        QProd
        (Q0
          (Just
            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))
        (Q0
          (Just
            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat")))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query15.sql",
  (Q1
    (QSort [(E0 (ESym "s_suppkey"))])
    (Q1
      (QProj
        [((ESym "s_suppkey"),(E0 (ESym "s_suppkey"))),
         ((ESym "s_name"),(E0 (ESym "s_name"))),
         ((ESym "s_address"),(E0 (ESym "s_address"))),
         ((ESym "s_phone"),(E0 (ESym "s_phone"))),
         ((ESym "total_revenue"),(E0 (ESym "total_revenue")))])
      (Q2
        QProjQuery
        (S
          (P0
            (R2 REq
                (R0 (E0 (ESym "s_suppkey")))
                (R0 (E0 (ESym "supplier_no")))))
          (Q2
            QProd
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
            (Q1
              (QGroup
                [((ESym "supplier_no"),
                  (E0 (NAggr AggrFirst (E0 (ESym "l_suppkey"))))),
                 ((ESym "total_revenue"),
                  (E0
                    (NAggr
                      AggrSum
                      (E2 EMul
                          (E0 (ESym "l_extendedprice"))
                          (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount")))))))]
                [(E0 (ESym "l_suppkey"))])
              (S
                (And
                  (P0
                    (R2
                      RGe
                      (R0 (E0 (ESym "l_shipdate")))
                      (R0
                        (E0
                          (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                  (P0
                    (R2
                      RLt
                      (R0 (E0 (ESym "l_shipdate")))
                      (R0
                        (E2
                          EAdd
                          (E0
                            (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                          (E0
                            (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))
        (Q2
          QDistinct
          (S
            (P0
              (R2 REq
                  (R0 (E0 (ESym "s_suppkey")))
                  (R0 (E0 (ESym "supplier_no")))))
            (Q2
              QProd
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
              (Q1
                (QGroup
                  [((ESym "supplier_no"),
                    (E0 (NAggr AggrFirst (E0 (ESym "l_suppkey"))))),
                   ((ESym "total_revenue"),
                    (E0
                      (NAggr
                        AggrSum
                        (E2
                          EMul
                          (E0 (ESym "l_extendedprice"))
                          (E2 ESub
                              (E0 (EInt 1))
                              (E0 (ESym "l_discount")))))))]
                  [(E0 (ESym "l_suppkey"))])
                (S
                  (And
                    (P0
                      (R2
                        RGe
                        (R0 (E0 (ESym "l_shipdate")))
                        (R0
                          (E0
                            (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                    (P0
                      (R2
                        RLt
                        (R0 (E0 (ESym "l_shipdate")))
                        (R0
                          (E2
                            EAdd
                            (E0
                              (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                            (E0
                              (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))
          (J
            (P0
              (R2 REq
                  (R0 (E0 (ESym "total_revenue")))
                  (R0 (E0 (ESym "unnested_query0")))))
            (S
              (P0
                (R2 REq
                    (R0 (E0 (ESym "s_suppkey")))
                    (R0 (E0 (ESym "supplier_no")))))
              (Q2
                QProd
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
                (Q1
                  (QGroup
                    [((ESym "supplier_no"),
                      (E0 (NAggr AggrFirst (E0 (ESym "l_suppkey"))))),
                     ((ESym "total_revenue"),
                      (E0
                        (NAggr
                          AggrSum
                          (E2
                            EMul
                            (E0 (ESym "l_extendedprice"))
                            (E2 ESub
                                (E0 (EInt 1))
                                (E0 (ESym "l_discount")))))))]
                    [(E0 (ESym "l_suppkey"))])
                  (S
                    (And
                      (P0
                        (R2
                          RGe
                          (R0 (E0 (ESym "l_shipdate")))
                          (R0
                            (E0
                              (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                      (P0
                        (R2
                          RLt
                          (R0 (E0 (ESym "l_shipdate")))
                          (R0
                            (E2
                              EAdd
                              (E0
                                (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                              (E0
                                (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))
            (Q1
              (QGroup [] [])
              (Q1
                (QGroup
                  [((ESym "supplier_no"),
                    (E0 (NAggr AggrFirst (E0 (ESym "l_suppkey"))))),
                   ((ESym "total_revenue"),
                    (E0
                      (NAggr
                        AggrSum
                        (E2
                          EMul
                          (E0 (ESym "l_extendedprice"))
                          (E2 ESub
                              (E0 (EInt 1))
                              (E0 (ESym "l_discount")))))))]
                  [(E0 (ESym "l_suppkey"))])
                (S
                  (And
                    (P0
                      (R2
                        RGe
                        (R0 (E0 (ESym "l_shipdate")))
                        (R0
                          (E0
                            (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                    (P0
                      (R2
                        RLt
                        (R0 (E0 (ESym "l_shipdate")))
                        (R0
                          (E2
                            EAdd
                            (E0
                              (EDate (Date {year = 1996, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                            (E0
                              (EInterval (Date {year = 0, month = 3, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query16.sql",
  (Q1
    (QLimit 10)
    (Q1
      (QProj
        [((ESym "p_brand"),(E0 (ESym "p_brand"))),
         ((ESym "p_type"),(E0 (ESym "p_type"))),
         ((ESym "p_size"),(E0 (ESym "p_size")))])
      (S
        (And
          (P0
            (R2 REq
                (R0 (E0 (ESym "p_partkey")))
                (R0 (E0 (ESym "ps_partkey")))))
          (And
            (Not
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_brand")))
                    (R0 (E0 (EString "Brand#45"))))))
            (And
              (Not
                (P0
                  (R2 RLike
                      (R0 (E0 (ESym "p_type")))
                      (R0 (E0 (EString "MEDIUM POLISHED%"))))))
              (Or
                (Or
                  (Or
                    (Or
                      (Or
                        (Or
                          (Or
                            (P0
                              (R2 REq
                                  (R0 (E0 (ESym "p_size")))
                                  (R0 (E0 (EInt 49)))))
                            (P0
                              (R2 REq
                                  (R0 (E0 (ESym "p_size")))
                                  (R0 (E0 (EInt 14))))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "p_size")))
                                (R0 (E0 (EInt 23))))))
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "p_size")))
                              (R0 (E0 (EInt 45))))))
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "p_size")))
                            (R0 (E0 (EInt 19))))))
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "p_size")))
                          (R0 (E0 (EInt 3))))))
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "p_size")))
                        (R0 (E0 (EInt 36))))))
                (P0 (R2 REq (R0 (E0 (ESym "p_size"))) (R0 (E0 (EInt 9)))))))))
        (Q2
          QProd
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat"))
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query17.sql",
  (Q1
    (QGroup
      [((ESym "avg_yearly"),
        (E2 EDiv
            (E0 (NAggr AggrSum (E0 (ESym "l_extendedprice"))))
            (E0 (NAggr AggrFirst (E0 (EFloat 7.0))))))]
      [])
    (Q2
      QProjQuery
      (S
        (And
          (P0
            (R2 REq
                (R0 (E0 (ESym "p_partkey")))
                (R0 (E0 (ESym "l_partkey")))))
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_brand")))
                  (R0 (E0 (EString "Brand#23")))))
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_container")))
                  (R0 (E0 (EString "MED BOX")))))))
        (Q2
          QProd
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))
          (Q0
            (Just
              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))))
      (Q2
        QDistinct
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_partkey")))
                  (R0 (E0 (ESym "l_partkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_brand")))
                    (R0 (E0 (EString "Brand#23")))))
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_container")))
                    (R0 (E0 (EString "MED BOX")))))))
          (Q2
            QProd
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))))
        (J
          (And
            (P0
              (R2 RLt
                  (R0 (E0 (ESym "l_quantity")))
                  (R0 (E0 (ESym "unnested_query0")))))
            (P0
              (R2 REq
                  (R0 (E0 (ESym "l.l_partkey")))
                  (R0 (E0 (ESym "p_partkey"))))))
          (S
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_partkey")))
                    (R0 (E0 (ESym "l_partkey")))))
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "p_brand")))
                      (R0 (E0 (EString "Brand#23")))))
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "p_container")))
                      (R0 (E0 (EString "MED BOX")))))))
            (Q2
              QProd
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))))
          (Q1
            (QGroup [] [(E0 (ESym "p_partkey"))])
            (Q1
              (QProj
                [((ESym "l.l_partkey"),(E0 (ESym "l_partkey"))),
                 ((ESym "l.l_quantity"),(E0 (ESym "l_quantity")))])
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query18.sql",
  (Q1
    (QLimit 100)
    (Q1
      (QSort
        [(E1 ENeg (E0 (ESym "o_totalprice"))),
         (E0 (ESym "o_orderdate"))])
      (Q1
        (QGroup
          [((ESym "c_name"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_name"))))),
           ((ESym "c_custkey"),
            (E0 (NAggr AggrFirst (E0 (ESym "c_custkey"))))),
           ((ESym "o_orderkey"),
            (E0 (NAggr AggrFirst (E0 (ESym "o_orderkey"))))),
           ((ESym "o_orderdate"),
            (E0 (NAggr AggrFirst (E0 (ESym "o_orderdate"))))),
           ((ESym "o_totalprice"),
            (E0 (NAggr AggrFirst (E0 (ESym "o_totalprice"))))),
           ((ESym "__sym14"),
            (E0 (NAggr AggrSum (E0 (ESym "l_quantity")))))]
          [(E0 (ESym "c_name")),
           (E0 (ESym "c_custkey")),
           (E0 (ESym "o_orderkey")),
           (E0 (ESym "o_orderdate")),
           (E0 (ESym "o_totalprice"))])
        (Q2
          QProjQuery
          (S
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "c_custkey")))
                    (R0 (E0 (ESym "o_custkey")))))
              (P0
                (R2 REq
                    (R0 (E0 (ESym "o_orderkey")))
                    (R0 (E0 (ESym "l_orderkey"))))))
            (Q2
              QProd
              (Q2
                QProd
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat"))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
          (Q2
            QDistinct
            (S
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "c_custkey")))
                      (R0 (E0 (ESym "o_custkey")))))
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "o_orderkey")))
                      (R0 (E0 (ESym "l_orderkey"))))))
              (Q2
                QProd
                (Q2
                  QProd
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat"))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
            (J
              (P0
                (R2 REq
                    (R0 (E0 (ESym "o_orderkey")))
                    (R0 (E0 (ESym "l.l_orderkey")))))
              (S
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "c_custkey")))
                        (R0 (E0 (ESym "o_custkey")))))
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "o_orderkey")))
                        (R0 (E0 (ESym "l_orderkey"))))))
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat"))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
              (Q1
                (QProj
                  [((ESym "l.l_orderkey"),(E0 (ESym "l.l_orderkey")))])
                (S
                  (P0
                    (R2 RGt
                        (R0 (E0 (ESym "having_aggregation13")))
                        (R0 (E0 (EInt 300)))))
                  (Q1
                    (QGroup
                      [((ESym "l.l_orderkey"),
                        (E0 (NAggr AggrFirst (E0 (ESym "l.l_orderkey"))))),
                       ((ESym "having_aggregation13"),
                        (E0 (NAggr AggrSum (E0 (ESym "l.l_quantity")))))]
                      [(E0 (ESym "l.l_orderkey"))])
                    (Q1
                      (QProj
                        [((ESym "l.l_orderkey"),
                          (E0 (ESym "l_orderkey")))])
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query19.sql",
  (Q1
    (QGroup
      [((ESym "revenue"),
        (E0
          (NAggr
            AggrSum
            (E2 EMul
                (E0 (ESym "l_extendedprice"))
                (E2 ESub (E0 (EInt 1)) (E0 (ESym "l_discount")))))))]
      [])
    (S
      (Or
        (And
          (P0
            (R2 REq
                (R0 (E0 (ESym "p_partkey")))
                (R0 (E0 (ESym "l_partkey")))))
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_brand")))
                  (R0 (E0 (EString "Brand#12")))))
            (And
              (Or
                (Or
                  (Or
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "p_container")))
                          (R0 (E0 (EString "SM CASE")))))
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "p_container")))
                          (R0 (E0 (EString "SM BOX"))))))
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "p_container")))
                        (R0 (E0 (EString "SM PACK"))))))
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "p_container")))
                      (R0 (E0 (EString "SM PKG"))))))
              (And
                (P0
                  (R2 RGe
                      (R0 (E0 (ESym "l_quantity")))
                      (R0 (E0 (EInt 1)))))
                (And
                  (P0
                    (R2 RLe
                        (R0 (E0 (ESym "l_quantity")))
                        (R0 (E2 EAdd (E0 (EInt 1)) (E0 (EInt 10))))))
                  (And
                    (And
                      (P0
                        (R2 RLe
                            (R0 (E0 (EInt 1)))
                            (R0 (E0 (ESym "p_size")))))
                      (P0
                        (R2 RLe
                            (R0 (E0 (ESym "p_size")))
                            (R0 (E0 (EInt 5))))))
                    (And
                      (Or
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "l_shipmode")))
                              (R0 (E0 (EString "AIR")))))
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "l_shipmode")))
                              (R0 (E0 (EString "AIR REG"))))))
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "l_shipinstruct")))
                            (R0 (E0 (EString "DELIVER IN PERSON"))))))))))))
        (Or
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_partkey")))
                  (R0 (E0 (ESym "l_partkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_brand")))
                    (R0 (E0 (EString "Brand#23")))))
              (And
                (Or
                  (Or
                    (Or
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "p_container")))
                            (R0 (E0 (EString "MED BAG")))))
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "p_container")))
                            (R0 (E0 (EString "MED BOX"))))))
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "p_container")))
                          (R0 (E0 (EString "MED PKG"))))))
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "p_container")))
                        (R0 (E0 (EString "MED PACK"))))))
                (And
                  (P0
                    (R2 RGe
                        (R0 (E0 (ESym "l_quantity")))
                        (R0 (E0 (EInt 10)))))
                  (And
                    (P0
                      (R2 RLe
                          (R0 (E0 (ESym "l_quantity")))
                          (R0 (E2 EAdd (E0 (EInt 10)) (E0 (EInt 10))))))
                    (And
                      (And
                        (P0
                          (R2 RLe
                              (R0 (E0 (EInt 1)))
                              (R0 (E0 (ESym "p_size")))))
                        (P0
                          (R2 RLe
                              (R0 (E0 (ESym "p_size")))
                              (R0 (E0 (EInt 10))))))
                      (And
                        (Or
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "l_shipmode")))
                                (R0 (E0 (EString "AIR")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "l_shipmode")))
                                (R0 (E0 (EString "AIR REG"))))))
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "l_shipinstruct")))
                              (R0 (E0 (EString "DELIVER IN PERSON"))))))))))))
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "p_partkey")))
                  (R0 (E0 (ESym "l_partkey")))))
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "p_brand")))
                    (R0 (E0 (EString "Brand#34")))))
              (And
                (Or
                  (Or
                    (Or
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "p_container")))
                            (R0 (E0 (EString "LG CASE")))))
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "p_container")))
                            (R0 (E0 (EString "LG BOX"))))))
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "p_container")))
                          (R0 (E0 (EString "LG PACK"))))))
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "p_container")))
                        (R0 (E0 (EString "LG PKG"))))))
                (And
                  (P0
                    (R2 RGe
                        (R0 (E0 (ESym "l_quantity")))
                        (R0 (E0 (EInt 20)))))
                  (And
                    (P0
                      (R2 RLe
                          (R0 (E0 (ESym "l_quantity")))
                          (R0 (E2 EAdd (E0 (EInt 20)) (E0 (EInt 10))))))
                    (And
                      (And
                        (P0
                          (R2 RLe
                              (R0 (E0 (EInt 1)))
                              (R0 (E0 (ESym "p_size")))))
                        (P0
                          (R2 RLe
                              (R0 (E0 (ESym "p_size")))
                              (R0 (E0 (EInt 15))))))
                      (And
                        (Or
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "l_shipmode")))
                                (R0 (E0 (EString "AIR")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "l_shipmode")))
                                (R0 (E0 (EString "AIR REG"))))))
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "l_shipinstruct")))
                              (R0 (E0 (EString "DELIVER IN PERSON"))))))))))))))
      (Q2
        QProd
        (Q0
          (Just
            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))
        (Q0
          (Just
            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat")))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query20.sql",
  (Q1
    (QSort [(E0 (ESym "s_name"))])
    (Q1
      (QProj
        [((ESym "s_name"),(E0 (ESym "s_name"))),
         ((ESym "s_address"),(E0 (ESym "s_address")))])
      (Q2
        QProjQuery
        (S
          (And
            (P0
              (R2 REq
                  (R0 (E0 (ESym "s_nationkey")))
                  (R0 (E0 (ESym "n_nationkey")))))
            (P0
              (R2 REq
                  (R0 (E0 (ESym "n_name")))
                  (R0 (E0 (EString "CANADA"))))))
          (Q2
            QProd
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
            (Q0
              (Just
                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
        (Q2
          QDistinct
          (S
            (And
              (P0
                (R2 REq
                    (R0 (E0 (ESym "s_nationkey")))
                    (R0 (E0 (ESym "n_nationkey")))))
              (P0
                (R2 REq
                    (R0 (E0 (ESym "n_name")))
                    (R0 (E0 (EString "CANADA"))))))
            (Q2
              QProd
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
          (J
            (P0
              (R2 REq
                  (R0 (E0 (ESym "s_suppkey")))
                  (R0 (E0 (ESym "ps.ps_suppkey")))))
            (S
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "s_nationkey")))
                      (R0 (E0 (ESym "n_nationkey")))))
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "n_name")))
                      (R0 (E0 (EString "CANADA"))))))
              (Q2
                QProd
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
            (Q1
              (QProj
                [((ESym "ps.ps_suppkey"),(E0 (ESym "ps.ps_suppkey")))])
              (Q2
                QProjQuery
                (Q2
                  QProjQuery
                  (Q1
                    (QProj
                      [((ESym "ps.ps_partkey"),
                        (E0 (ESym "ps_partkey"))),
                       ((ESym "ps.ps_availqty"),
                        (E0 (ESym "ps_availqty"))),
                       ((ESym "ps.ps_suppkey"),
                        (E0 (ESym "ps_suppkey")))])
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                  (Q2
                    QDistinct
                    (Q1
                      (QProj
                        [((ESym "ps.ps_partkey"),
                          (E0 (ESym "ps_partkey"))),
                         ((ESym "ps.ps_availqty"),
                          (E0 (ESym "ps_availqty"))),
                         ((ESym "ps.ps_suppkey"),
                          (E0 (ESym "ps_suppkey")))])
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                    (J
                      (And
                        (And
                          (P0
                            (R2 RGt
                                (R0 (E0 (ESym "ps.ps_availqty")))
                                (R0 (E0 (ESym "unnested_query0")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "l.l_partkey")))
                                (R0 (E0 (ESym "ps.ps_partkey"))))))
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "l.l_suppkey")))
                              (R0 (E0 (ESym "ps.ps_suppkey"))))))
                      (Q1
                        (QProj
                          [((ESym "ps.ps_partkey"),
                            (E0 (ESym "ps_partkey"))),
                           ((ESym "ps.ps_availqty"),
                            (E0 (ESym "ps_availqty"))),
                           ((ESym "ps.ps_suppkey"),
                            (E0 (ESym "ps_suppkey")))])
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                      (Q1
                        (QGroup
                          []
                          [(E0 (ESym "ps.ps_partkey")),
                           (E0 (ESym "ps.ps_suppkey"))])
                        (S
                          (And
                            (P0
                              (R2
                                RGe
                                (R0 (E0 (ESym "l.l_shipdate")))
                                (R0
                                  (E0
                                    (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                            (P0
                              (R2
                                RLt
                                (R0 (E0 (ESym "l.l_shipdate")))
                                (R0
                                  (E2
                                    EAdd
                                    (E0
                                      (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                                    (E0
                                      (EInterval (Date {year = 1, month = 0, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
                          (Q1
                            (QProj
                              [((ESym "l.l_partkey"),
                                (E0 (ESym "l_partkey"))),
                               ((ESym "l.l_suppkey"),
                                (E0 (ESym "l_suppkey"))),
                               ((ESym "l.l_shipdate"),
                                (E0 (ESym "l_shipdate"))),
                               ((ESym "l.l_quantity"),
                                (E0 (ESym "l_quantity")))])
                            (Q0
                              (Just
                                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))))
                (Q2
                  QDistinct
                  (Q2
                    QProjQuery
                    (Q1
                      (QProj
                        [((ESym "ps.ps_partkey"),
                          (E0 (ESym "ps_partkey"))),
                         ((ESym "ps.ps_availqty"),
                          (E0 (ESym "ps_availqty"))),
                         ((ESym "ps.ps_suppkey"),
                          (E0 (ESym "ps_suppkey")))])
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                    (Q2
                      QDistinct
                      (Q1
                        (QProj
                          [((ESym "ps.ps_partkey"),
                            (E0 (ESym "ps_partkey"))),
                           ((ESym "ps.ps_availqty"),
                            (E0 (ESym "ps_availqty"))),
                           ((ESym "ps.ps_suppkey"),
                            (E0 (ESym "ps_suppkey")))])
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                      (J
                        (And
                          (And
                            (P0
                              (R2 RGt
                                  (R0 (E0 (ESym "ps.ps_availqty")))
                                  (R0 (E0 (ESym "unnested_query0")))))
                            (P0
                              (R2 REq
                                  (R0 (E0 (ESym "l.l_partkey")))
                                  (R0 (E0 (ESym "ps.ps_partkey"))))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "l.l_suppkey")))
                                (R0 (E0 (ESym "ps.ps_suppkey"))))))
                        (Q1
                          (QProj
                            [((ESym "ps.ps_partkey"),
                              (E0 (ESym "ps_partkey"))),
                             ((ESym "ps.ps_availqty"),
                              (E0 (ESym "ps_availqty"))),
                             ((ESym "ps.ps_suppkey"),
                              (E0 (ESym "ps_suppkey")))])
                          (Q0
                            (Just
                              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                        (Q1
                          (QGroup
                            []
                            [(E0 (ESym "ps.ps_partkey")),
                             (E0 (ESym "ps.ps_suppkey"))])
                          (S
                            (And
                              (P0
                                (R2
                                  RGe
                                  (R0 (E0 (ESym "l.l_shipdate")))
                                  (R0
                                    (E0
                                      (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                              (P0
                                (R2
                                  RLt
                                  (R0 (E0 (ESym "l.l_shipdate")))
                                  (R0
                                    (E2
                                      EAdd
                                      (E0
                                        (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                                      (E0
                                        (EInterval (Date {year = 1, month = 0, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
                            (Q1
                              (QProj
                                [((ESym "l.l_partkey"),
                                  (E0 (ESym "l_partkey"))),
                                 ((ESym "l.l_suppkey"),
                                  (E0 (ESym "l_suppkey"))),
                                 ((ESym "l.l_shipdate"),
                                  (E0 (ESym "l_shipdate"))),
                                 ((ESym "l.l_quantity"),
                                  (E0 (ESym "l_quantity")))])
                              (Q0
                                (Just
                                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))))
                  (J
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "ps.ps_partkey")))
                          (R0 (E0 (ESym "p.p_partkey")))))
                    (Q2
                      QProjQuery
                      (Q1
                        (QProj
                          [((ESym "ps.ps_partkey"),
                            (E0 (ESym "ps_partkey"))),
                           ((ESym "ps.ps_availqty"),
                            (E0 (ESym "ps_availqty"))),
                           ((ESym "ps.ps_suppkey"),
                            (E0 (ESym "ps_suppkey")))])
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                      (Q2
                        QDistinct
                        (Q1
                          (QProj
                            [((ESym "ps.ps_partkey"),
                              (E0 (ESym "ps_partkey"))),
                             ((ESym "ps.ps_availqty"),
                              (E0 (ESym "ps_availqty"))),
                             ((ESym "ps.ps_suppkey"),
                              (E0 (ESym "ps_suppkey")))])
                          (Q0
                            (Just
                              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                        (J
                          (And
                            (And
                              (P0
                                (R2 RGt
                                    (R0 (E0 (ESym "ps.ps_availqty")))
                                    (R0 (E0 (ESym "unnested_query0")))))
                              (P0
                                (R2 REq
                                    (R0 (E0 (ESym "l.l_partkey")))
                                    (R0 (E0 (ESym "ps.ps_partkey"))))))
                            (P0
                              (R2 REq
                                  (R0 (E0 (ESym "l.l_suppkey")))
                                  (R0 (E0 (ESym "ps.ps_suppkey"))))))
                          (Q1
                            (QProj
                              [((ESym "ps.ps_partkey"),
                                (E0 (ESym "ps_partkey"))),
                               ((ESym "ps.ps_availqty"),
                                (E0 (ESym "ps_availqty"))),
                               ((ESym "ps.ps_suppkey"),
                                (E0 (ESym "ps_suppkey")))])
                            (Q0
                              (Just
                                "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/partsupp.dat")))
                          (Q1
                            (QGroup
                              []
                              [(E0 (ESym "ps.ps_partkey")),
                               (E0 (ESym "ps.ps_suppkey"))])
                            (S
                              (And
                                (P0
                                  (R2
                                    RGe
                                    (R0 (E0 (ESym "l.l_shipdate")))
                                    (R0
                                      (E0
                                        (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0}))))))
                                (P0
                                  (R2
                                    RLt
                                    (R0 (E0 (ESym "l.l_shipdate")))
                                    (R0
                                      (E2
                                        EAdd
                                        (E0
                                          (EDate (Date {year = 1994, month = 1, day = 1, hour = 0, minute = 0, seconds = 0})))
                                        (E0
                                          (EInterval (Date {year = 1, month = 0, day = 0, hour = 0, minute = 0, seconds = 0}))))))))
                              (Q1
                                (QProj
                                  [((ESym "l.l_partkey"),
                                    (E0 (ESym "l_partkey"))),
                                   ((ESym "l.l_suppkey"),
                                    (E0 (ESym "l_suppkey"))),
                                   ((ESym "l.l_shipdate"),
                                    (E0 (ESym "l_shipdate"))),
                                   ((ESym "l.l_quantity"),
                                    (E0 (ESym "l_quantity")))])
                                (Q0
                                  (Just
                                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))))
                    (Q1
                      (QProj
                        [((ESym "p.p_partkey"),
                          (E0 (ESym "p.p_partkey")))])
                      (S
                        (P0
                          (R2 RLike
                              (R0 (E0 (ESym "p.p_name")))
                              (R0 (E0 (EString "forest%")))))
                        (Q1
                          (QProj
                            [((ESym "p.p_name"),(E0 (ESym "p_name"))),
                             ((ESym "p.p_partkey"),
                              (E0 (ESym "p_partkey")))])
                          (Q0
                            (Just
                              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/part.dat"))))))))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query21.sql",
  (Q1
    (QLimit 100)
    (Q1
      (QSort [(E1 ENeg (E0 (ESym "numwait"))),(E0 (ESym "s_name"))])
      (Q1
        (QGroup
          [((ESym "s_name"),
            (E0 (NAggr AggrFirst (E0 (ESym "s_name"))))),
           ((ESym "numwait"),
            (E0 (NAggr AggrFirst (E0 (ECount Nothing)))))]
          [(E0 (ESym "s_name"))])
        (Q2
          QProjQuery
          (Q2
            (QLeftAntijoin
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "l3.l_orderkey")))
                      (R0 (E0 (ESym "l1.l_orderkey"))))))
            (S
              (And
                (P0
                  (R2 REq
                      (R0 (E0 (ESym "s_suppkey")))
                      (R0 (E0 (ESym "l1.l_suppkey")))))
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "o_orderkey")))
                        (R0 (E0 (ESym "l1.l_orderkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "o_orderstatus")))
                          (R0 (E0 (EString "F")))))
                    (And
                      (P0
                        (R2 RGt
                            (R0 (E0 (ESym "l1.l_receiptdate")))
                            (R0 (E0 (ESym "l1.l_commitdate")))))
                      (And
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "s_nationkey")))
                              (R0 (E0 (ESym "n_nationkey")))))
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "n_name")))
                              (R0 (E0 (EString "SAUDI ARABIA"))))))))))
              (Q2
                QProd
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
                    (Q1
                      (QProj
                        [((ESym "l1.l_suppkey"),
                          (E0 (ESym "l_suppkey"))),
                         ((ESym "l1.l_orderkey"),
                          (E0 (ESym "l_orderkey"))),
                         ((ESym "l1.l_receiptdate"),
                          (E0 (ESym "l_receiptdate"))),
                         ((ESym "l1.l_commitdate"),
                          (E0 (ESym "l_commitdate")))])
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
            (Q1
              (QProj
                [((ESym "l3.l_orderkey"),(E0 (ESym "l_orderkey"))),
                 ((ESym "l3.l_suppkey"),(E0 (ESym "l_suppkey"))),
                 ((ESym "l3.l_receiptdate"),
                  (E0 (ESym "l_receiptdate"))),
                 ((ESym "l3.l_commitdate"),(E0 (ESym "l_commitdate")))])
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
          (Q2
            QDistinct
            (Q2
              (QLeftAntijoin
               (P0
                 (R2 REq
                   (R0 (E0 (ESym "l3.l_orderkey")))
                   (R0 (E0 (ESym "l1.l_orderkey"))))))
              (S
                (And
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "s_suppkey")))
                        (R0 (E0 (ESym "l1.l_suppkey")))))
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "o_orderkey")))
                          (R0 (E0 (ESym "l1.l_orderkey")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "o_orderstatus")))
                            (R0 (E0 (EString "F")))))
                      (And
                        (P0
                          (R2 RGt
                              (R0 (E0 (ESym "l1.l_receiptdate")))
                              (R0 (E0 (ESym "l1.l_commitdate")))))
                        (And
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "s_nationkey")))
                                (R0 (E0 (ESym "n_nationkey")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (ESym "n_name")))
                                (R0 (E0 (EString "SAUDI ARABIA"))))))))))
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q2
                      QProd
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
                      (Q1
                        (QProj
                          [((ESym "l1.l_suppkey"),
                            (E0 (ESym "l_suppkey"))),
                           ((ESym "l1.l_orderkey"),
                            (E0 (ESym "l_orderkey"))),
                           ((ESym "l1.l_receiptdate"),
                            (E0 (ESym "l_receiptdate"))),
                           ((ESym "l1.l_commitdate"),
                            (E0 (ESym "l_commitdate")))])
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
              (Q1
                (QProj
                  [((ESym "l3.l_orderkey"),(E0 (ESym "l_orderkey"))),
                   ((ESym "l3.l_suppkey"),(E0 (ESym "l_suppkey"))),
                   ((ESym "l3.l_receiptdate"),
                    (E0 (ESym "l_receiptdate"))),
                   ((ESym "l3.l_commitdate"),
                    (E0 (ESym "l_commitdate")))])
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
            (J
              (P0
                (R2 REq
                    (R0 (E0 (ESym "l2.l_orderkey")))
                    (R0 (E0 (ESym "l1.l_orderkey")))))
              (Q2
                (QLeftAntijoin
                  (P0
                    (R2 REq
                      (R0 (E0 (ESym "l3.l_orderkey")))
                      (R0 (E0 (ESym "l1.l_orderkey"))))))
                (S
                  (And
                    (P0
                      (R2 REq
                          (R0 (E0 (ESym "s_suppkey")))
                          (R0 (E0 (ESym "l1.l_suppkey")))))
                    (And
                      (P0
                        (R2 REq
                            (R0 (E0 (ESym "o_orderkey")))
                            (R0 (E0 (ESym "l1.l_orderkey")))))
                      (And
                        (P0
                          (R2 REq
                              (R0 (E0 (ESym "o_orderstatus")))
                              (R0 (E0 (EString "F")))))
                        (And
                          (P0
                            (R2 RGt
                                (R0 (E0 (ESym "l1.l_receiptdate")))
                                (R0 (E0 (ESym "l1.l_commitdate")))))
                          (And
                            (P0
                              (R2 REq
                                  (R0 (E0 (ESym "s_nationkey")))
                                  (R0 (E0 (ESym "n_nationkey")))))
                            (P0
                              (R2 REq
                                  (R0 (E0 (ESym "n_name")))
                                  (R0 (E0 (EString "SAUDI ARABIA"))))))))))
                  (Q2
                    QProd
                    (Q2
                      QProd
                      (Q2
                        QProd
                        (Q0
                          (Just
                            "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/supplier.dat"))
                        (Q1
                          (QProj
                            [((ESym "l1.l_suppkey"),
                              (E0 (ESym "l_suppkey"))),
                             ((ESym "l1.l_orderkey"),
                              (E0 (ESym "l_orderkey"))),
                             ((ESym "l1.l_receiptdate"),
                              (E0 (ESym "l_receiptdate"))),
                             ((ESym "l1.l_commitdate"),
                              (E0 (ESym "l_commitdate")))])
                          (Q0
                            (Just
                              "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
                      (Q0
                        (Just
                          "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat")))
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/nation.dat"))))
                (Q1
                  (QProj
                    [((ESym "l3.l_orderkey"),(E0 (ESym "l_orderkey"))),
                     ((ESym "l3.l_suppkey"),(E0 (ESym "l_suppkey"))),
                     ((ESym "l3.l_receiptdate"),
                      (E0 (ESym "l_receiptdate"))),
                     ((ESym "l3.l_commitdate"),
                      (E0 (ESym "l_commitdate")))])
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))
              (S
                (Not
                  (P0
                    (R2 REq
                        (R0 (E0 (ESym "l2.l_suppkey")))
                        (R0 (E0 (ESym "l1.l_suppkey"))))))
                (Q1
                  (QProj
                    [((ESym "l2.l_orderkey"),(E0 (ESym "l_orderkey"))),
                     ((ESym "l2.l_suppkey"),(E0 (ESym "l_suppkey")))])
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/lineitem.dat"))))))))))),
 ("/home/drninjabatman/Projects/FluiDB/resources/tpch_query22.sql",
  (Q1
    (QSort [(E0 (ESym "cntrycode"))])
    (Q1
      (QGroup
        [((ESym "cntrycode"),
          (E0 (NAggr AggrFirst (E0 (ESym "cntrycode"))))),
         ((ESym "numcust"),
          (E0 (NAggr AggrFirst (E0 (ECount Nothing))))),
         ((ESym "totacctbal"),
          (E0 (NAggr AggrSum (E0 (ESym "c_acctbal")))))]
        [(E0 (ESym "cntrycode"))])
      (Q1
        (QProj
          [((ESym "cntrycode"),
            (E1 (EFun (SubSeq 1 3)) (E0 (ESym "c_phone")))),
           ((ESym "c_acctbal"),(E0 (ESym "c_acctbal")))])
        (Q2
          QProjQuery
          (Q2
            (QLeftAntijoin
              (P0
                (R2 REq
                  (R0 (E0 (ESym "o.o_custkey")))
                  (R0 (E0 (ESym "c_custkey"))))))
            (S
              (Or
                (Or
                  (Or
                    (Or
                      (Or
                        (Or
                          (P0
                            (R2
                              REq
                              (R0
                                (E1 (EFun (SubSeq 1 3))
                                    (E0 (ESym "c_phone"))))
                              (R0 (E0 (EString "13")))))
                          (P0
                            (R2
                              REq
                              (R0
                                (E1 (EFun (SubSeq 1 3))
                                    (E0 (ESym "c_phone"))))
                              (R0 (E0 (EString "31"))))))
                        (P0
                          (R2
                            REq
                            (R0
                              (E1 (EFun (SubSeq 1 3))
                                  (E0 (ESym "c_phone"))))
                            (R0 (E0 (EString "23"))))))
                      (P0
                        (R2
                          REq
                          (R0
                            (E1 (EFun (SubSeq 1 3))
                                (E0 (ESym "c_phone"))))
                          (R0 (E0 (EString "29"))))))
                    (P0
                      (R2
                        REq
                        (R0 (E1 (EFun (SubSeq 1 3)) (E0 (ESym "c_phone"))))
                        (R0 (E0 (EString "30"))))))
                  (P0
                    (R2
                      REq
                      (R0 (E1 (EFun (SubSeq 1 3)) (E0 (ESym "c_phone"))))
                      (R0 (E0 (EString "18"))))))
                (P0
                  (R2 REq
                      (R0 (E1 (EFun (SubSeq 1 3)) (E0 (ESym "c_phone"))))
                      (R0 (E0 (EString "17"))))))
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat")))
            (Q1
              (QProj [((ESym "o.o_custkey"),(E0 (ESym "o_custkey")))])
              (Q0
                (Just
                  "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat"))))
          (Q2
            QDistinct
            (Q2
              (QLeftAntijoin
                (P0
                  (R2 REq
                    (R0 (E0 (ESym "o.o_custkey")))
                    (R0 (E0 (ESym "c_custkey"))))))
              (S
                (Or
                  (Or
                    (Or
                      (Or
                        (Or
                          (Or
                            (P0
                              (R2
                                REq
                                (R0
                                  (E1 (EFun (SubSeq 1 3))
                                      (E0 (ESym "c_phone"))))
                                (R0 (E0 (EString "13")))))
                            (P0
                              (R2
                                REq
                                (R0
                                  (E1 (EFun (SubSeq 1 3))
                                      (E0 (ESym "c_phone"))))
                                (R0 (E0 (EString "31"))))))
                          (P0
                            (R2
                              REq
                              (R0
                                (E1 (EFun (SubSeq 1 3))
                                    (E0 (ESym "c_phone"))))
                              (R0 (E0 (EString "23"))))))
                        (P0
                          (R2
                            REq
                            (R0
                              (E1 (EFun (SubSeq 1 3))
                                  (E0 (ESym "c_phone"))))
                            (R0 (E0 (EString "29"))))))
                      (P0
                        (R2
                          REq
                          (R0
                            (E1 (EFun (SubSeq 1 3))
                                (E0 (ESym "c_phone"))))
                          (R0 (E0 (EString "30"))))))
                    (P0
                      (R2
                        REq
                        (R0 (E1 (EFun (SubSeq 1 3)) (E0 (ESym "c_phone"))))
                        (R0 (E0 (EString "18"))))))
                  (P0
                    (R2
                      REq
                      (R0 (E1 (EFun (SubSeq 1 3)) (E0 (ESym "c_phone"))))
                      (R0 (E0 (EString "17"))))))
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat")))
              (Q1
                (QProj [((ESym "o.o_custkey"),(E0 (ESym "o_custkey")))])
                (Q0
                  (Just
                    "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat"))))
            (J
              (P0
                (R2 RGt
                    (R0 (E0 (ESym "c_acctbal")))
                    (R0 (E0 (ESym "unnested_query0")))))
              (Q2
                (QLeftAntijoin
                  (P0
                    (R2 REq
                      (R0 (E0 (ESym "o.o_custkey")))
                      (R0 (E0 (ESym "c_custkey"))))))
                (S
                  (Or
                    (Or
                      (Or
                        (Or
                          (Or
                            (Or
                              (P0
                                (R2
                                  REq
                                  (R0
                                    (E1 (EFun (SubSeq 1 3))
                                        (E0 (ESym "c_phone"))))
                                  (R0 (E0 (EString "13")))))
                              (P0
                                (R2
                                  REq
                                  (R0
                                    (E1 (EFun (SubSeq 1 3))
                                        (E0 (ESym "c_phone"))))
                                  (R0 (E0 (EString "31"))))))
                            (P0
                              (R2
                                REq
                                (R0
                                  (E1 (EFun (SubSeq 1 3))
                                      (E0 (ESym "c_phone"))))
                                (R0 (E0 (EString "23"))))))
                          (P0
                            (R2
                              REq
                              (R0
                                (E1 (EFun (SubSeq 1 3))
                                    (E0 (ESym "c_phone"))))
                              (R0 (E0 (EString "29"))))))
                        (P0
                          (R2
                            REq
                            (R0
                              (E1 (EFun (SubSeq 1 3))
                                  (E0 (ESym "c_phone"))))
                            (R0 (E0 (EString "30"))))))
                      (P0
                        (R2
                          REq
                          (R0
                            (E1 (EFun (SubSeq 1 3))
                                (E0 (ESym "c_phone"))))
                          (R0 (E0 (EString "18"))))))
                    (P0
                      (R2
                        REq
                        (R0 (E1 (EFun (SubSeq 1 3)) (E0 (ESym "c_phone"))))
                        (R0 (E0 (EString "17"))))))
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat")))
                (Q1
                  (QProj [((ESym "o.o_custkey"),(E0 (ESym "o_custkey")))])
                  (Q0
                    (Just
                      "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/orders.dat"))))
              (Q1
                (QGroup [] [])
                (S
                  (And
                    (P0
                      (R2 RGt
                          (R0 (E0 (ESym "c.c_acctbal")))
                          (R0 (E0 (EFloat 0.0)))))
                    (Or
                      (Or
                        (Or
                          (Or
                            (Or
                              (Or
                                (P0
                                  (R2
                                    REq
                                    (R0
                                      (E1 (EFun (SubSeq 1 3))
                                          (E0 (ESym "c.c_phone"))))
                                    (R0 (E0 (EString "13")))))
                                (P0
                                  (R2
                                    REq
                                    (R0
                                      (E1 (EFun (SubSeq 1 3))
                                          (E0 (ESym "c.c_phone"))))
                                    (R0 (E0 (EString "31"))))))
                              (P0
                                (R2
                                  REq
                                  (R0
                                    (E1 (EFun (SubSeq 1 3))
                                        (E0 (ESym "c.c_phone"))))
                                  (R0 (E0 (EString "23"))))))
                            (P0
                              (R2
                                REq
                                (R0
                                  (E1 (EFun (SubSeq 1 3))
                                      (E0 (ESym "c.c_phone"))))
                                (R0 (E0 (EString "29"))))))
                          (P0
                            (R2
                              REq
                              (R0
                                (E1 (EFun (SubSeq 1 3))
                                    (E0 (ESym "c.c_phone"))))
                              (R0 (E0 (EString "30"))))))
                        (P0
                          (R2
                            REq
                            (R0
                              (E1 (EFun (SubSeq 1 3))
                                  (E0 (ESym "c.c_phone"))))
                            (R0 (E0 (EString "18"))))))
                      (P0
                        (R2
                          REq
                          (R0
                            (E1 (EFun (SubSeq 1 3))
                                (E0 (ESym "c.c_phone"))))
                          (R0 (E0 (EString "17")))))))
                  (Q1
                    (QProj
                      [((ESym "c.c_acctbal"),(E0 (ESym "c_acctbal"))),
                       ((ESym "c.c_phone"),(E0 (ESym "c_phone")))])
                    (Q0
                      (Just
                        "/home/drninjabatman/Projects/FluiDB/resources/tpch_data/customer.dat"))))))))))))]
