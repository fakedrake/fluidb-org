{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module FluiDB.Schema.SSB.Queries
  (ssbQueriesMap
  ,SSBField
  ,SSBTable
  ,ssbFldSchema
  ,ssbTpchDBGenConf
  ,ssbSchema
  ,ssbPrimKeys) where

import           Control.Monad
import           Data.CppAst.CodeSymbol
import           Data.CppAst.CppType
import           Data.Foldable
import qualified Data.IntMap                  as IM
import           Data.List
import           Data.Query.Algebra
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.Parser
import           Data.Query.SQL.Types
import           Data.String
import           Data.Utils.Functors
import           FluiDB.Bamify.Types
import           FluiDB.Schema.TPCH.Schemata

data IsInTable
  = IsLiteral
  | InTable
  | NotInTable
  deriving Eq
fromIsInTable :: IsInTable -> Maybe Bool
fromIsInTable = \case
  IsLiteral  -> Nothing
  InTable    -> Just True
  NotInTable -> Just False

inTable :: ExpTypeSym -> Table -> IsInTable
inTable = \case
  ESym sym -> \case
    NoTable -> IsLiteral -- Not actually a literal but what can you
                          -- do.
    TSymbol tbl -> if tblPrefix tbl `isPrefixOf` sym
      then InTable else NotInTable
  _ -> const IsLiteral

type SSBField = String
type SSBTable = String
tblPrefix :: SSBField -> String
tblPrefix = \case
  "lineorder" -> "lo_"
  "dwdate"    -> "d_"
  "part"      -> "p_"
  "supplier"  -> "s_"
  "customer"  -> "c_"
  tbln        -> error "unknown table: " ++ tbln

allFields :: [SSBField]
allFields = nub $ do
  q <- ssbQueriesList
  e <- toList $ FlipQuery q
  case e of
    ESym sym -> return sym
    _        -> []

allTables :: [SSBTable]
allTables = nub $ do
  t <- ssbQueriesList >>= toList
  case t of
    NoTable     -> []
    TSymbol tbl -> return tbl

ssbFldSchema :: [(SSBTable,[SSBField])]
ssbFldSchema = do
  tbl <- allTables
  let tblFields =
        filter (\fld -> inTable (ESym fld) (TSymbol tbl) == InTable) allFields
  return (tbl,tblFields)


-- O(n^2) but it's done once
ssbSchema :: [(SSBTable,CppSchema' SSBField)]
ssbSchema = appendOrderLines $ do
  (tblTpch,schTpch) <- tpchSchemaAssoc
  guard $ tblTpch /= "lineitems" && tblTpch /= "orders"
  (tbl,flds) <- ssbFldSchema
  guard $ tbl /= "lineorder"
  let ssbSch = do
        guard $ tbl == tblTpch
        fld <- flds
        (tyTpch,eTpch) <- schTpch
        guard $ eTpch == fromString fld
        return (tyTpch,eTpch)
  return (tbl,ssbSch)
  where
    appendOrderLines
      :: [(SSBTable,CppSchema' SSBField)] -> [(SSBTable,CppSchema' SSBField)]
    appendOrderLines xs = ("lineorder",lineorderSchema) : xs
    lineorderSchema =
      [(CppNat,"lo_orderkey")
      ,(CppNat,"lo_partkey")
      ,(CppNat,"lo_suppkey")
      ,(CppInt,"lo_linenumber")
      ,(CppNat,"lo_quantity")
      ,(CppDouble,"lo_extendedprice")
      ,(CppDouble,"lo_discount")
      ,(CppDouble,"lo_tax")
      ,(strType 1,"lo_returnflag")
      ,(strType 1,"lo_linestatus")
      ,(dateType,"lo_shipdate")
      ,(dateType,"lo_commitdate")
      ,(dateType,"lo_receiptdate")
      ,(strType 25,"lo_shipinstruct")
      ,(strType 10,"lo_shipmode")
      ,(strType 44,"lo_comment")
      ,(CppNat,"lo_orderkey")
      ,(CppNat,"lo_custkey")
      ,(strType 1,"lo_orderstatus")
      ,(CppDouble,"lo_totalprice")
      ,(dateType,"lo_orderdate")
      ,(strType 15,"lo_orderpriority")
      ,(strType 15,"lo_clerk")
      ,(CppInt,"lo_shippriority")
      ,(strType 79,"lo_comment")]

strType :: Int -> CppType
strType = CppArray CppChar . LiteralSize
dateType :: CppType
dateType = CppNat

-- | Except for the star index (which is lineorder) all other fields
-- that end with "key" are primary keys.
ssbPrimKeys :: [(SSBTable,[SSBField])]
ssbPrimKeys = do
  (tbl,flds) <- ssbFldSchema
  return (tbl,if tbl /= "lineoder" then [] else filter (isSuffixOf "key") flds)

ssbQueriesMap :: IM.IntMap (Query ExpTypeSym Table)
ssbQueriesMap = IM.fromList $ zip [0 ..] ssbQueriesList

ssbQueriesList :: [Query ExpTypeSym Table]
ssbQueriesList =
  fmap
    (fromRightStr . parseSQL inQuery . unwords)
    [["select sum(lo_extendedprice*lo_discount) as revenue"
     ,"from lineorder, dwdate"
     ,"where lo_orderdate = d_datekey"
     ,"and d_yearmonthnum = 199401"
     ,"and lo_discount between 4 and 6"
     ,"and lo_quantity between 26 and 35"]
    ,["select sum(lo_extendedprice*lo_discount) as revenue"
     ,"from lineorder, dwdate"
     ,"where lo_orderdate = d_datekey"
     ,"and d_year = 1993"
     ,"and lo_discount between 1 and 3"
     ,"and lo_quantity < 25"]
    ,["select sum(lo_extendedprice*lo_discount) as revenue"
     ,"from lineorder, dwdate"
     ,"where lo_orderdate = d_datekey"
     ,"and d_yearmonthnum = 199401"
     ,"and lo_discount between 4 and 6"
     ,"and lo_quantity between 26 and 35"]
    ,["select sum(lo_extendedprice*lo_discount) as revenue"
     ,"from lineorder, dwdate"
     ,"where lo_orderdate = d_datekey"
     ,"and d_weeknuminyear = 6"
     ,"and d_year = 1994"
     ,"and lo_discount between 5 and 7"
     ,"and lo_quantity between 26 and 35"]
    ,["select sum(lo_revenue), d_year, p_brand1"
     ,"from lineorder, dwdate, part, supplier"
     ,"where lo_orderdate = d_datekey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and p_category = 'MFGR#12'"
     ,"and s_region = 'AMERICA'"
     ,"group by d_year, p_brand1"
     ,"order by d_year, p_brand1"]
    ,["select sum(lo_revenue), d_year, p_brand1"
     ,"from lineorder, dwdate, part, supplier"
     ,"where lo_orderdate = d_datekey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and p_brand1 between 'MFGR#2221' and 'MFGR#2228'"
     ,"and s_region = 'ASIA'"
     ,"group by d_year, p_brand1"
     ,"order by d_year, p_brand1"]
    ,["select sum(lo_revenue), d_year, p_brand1"
     ,"from lineorder, dwdate, part, supplier"
     ,"where lo_orderdate = d_datekey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and p_brand1 = 'MFGR#2221'"
     ,"and s_region = 'EUROPE'"
     ,"group by d_year, p_brand1"
     ,"order by d_year, p_brand1"]
    ,["select c_nation, s_nation, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, dwdate"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and c_region = 'ASIA' and s_region = 'ASIA'"
     ,"and d_year >= 1992 and d_year <= 1997"
     ,"group by c_nation, s_nation, d_year"
     ,"order by d_year, revenue desc"]
    ,["select c_city, s_city, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, dwdate"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and c_nation = 'UNITED STATES'"
     ,"and s_nation = 'UNITED STATES'"
     ,"and d_year >= 1992 and d_year <= 1997"
     ,"group by c_city, s_city, d_year"
     ,"order by d_year, revenue desc"]
    ,["select c_city, s_city, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, dwdate"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and (c_city='UNITED KI1' or c_city='UNITED KI5')"
     ,"and (s_city='UNITED KI1' or s_city='UNITED KI5')"
     ,"and d_year >= 1992 and d_year <= 1997"
     ,"group by c_city, s_city, d_year"
     ,"order by d_year, revenue desc"]
    ,["select c_city, s_city, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, dwdate"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and (c_city='UNITED KI1' or c_city='UNITED KI5')"
     ,"and (s_city='UNITED KI1' or s_city='UNITED KI5')"
     ,"and d_yearmonth = 'Dec1997'"
     ,"group by c_city, s_city, d_year"
     ,"order by d_year, revenue desc"]
    ,["select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit"
     ,"from dwdate, customer, supplier, part, lineorder"
     ,"where lo_custkey = c_custkey"
     ," and lo_suppkey = s_suppkey"
     ," and lo_partkey = p_partkey"
     ," and lo_orderdate = d_datekey"
     ," and c_region = 'AMERICA'"
     ," and s_region = 'AMERICA'"
     ," and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')"
     ,"group by d_year, c_nation"
     ,"order by d_year, c_nation"]
    ,["select d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) as profit"
     ,"from dwdate, customer, supplier, part, lineorder"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_orderdate = d_datekey"
     ,"and c_region = 'AMERICA'"
     ,"and s_region = 'AMERICA'"
     ,"and (d_year = 1997 or d_year = 1998)"
     ,"and (p_mfgr = 'MFGR#1'"
     ,"or p_mfgr = 'MFGR#2')"
     ,"group by d_year, s_nation, p_category order by d_year, s_nation, p_category"]
    ,["select d_year, s_city, p_brand1, sum(lo_revenue - lo_supplycost) as profit"
     ,"from dwdate, customer, supplier, part, lineorder"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_orderdate = d_datekey"
     ,"and c_region = 'AMERICA'"
     ,"and s_nation = 'UNITED STATES'"
     ,"and (d_year = 1997 or d_year = 1998)"
     ,"and p_category = 'MFGR#14'"
     ,"group by d_year, s_city, p_brand1 order by d_year, s_city, p_brand1"]]

fromRightStr
  :: Either String (Query ExpTypeSym Table) -> Query ExpTypeSym Table
fromRightStr = \case
  Left str -> error $ "Failed parsing: " ++ str
  Right q  -> q

inQuery :: ExpTypeSym -> Query ExpTypeSym Table -> Maybe Bool
inQuery e = fmap and . traverse (fromIsInTable . inTable e)

ssbTpchDBGenConf :: [SSBTable] -> DBGenConf
ssbTpchDBGenConf Tblnames =
  DBGenConf
  { dbGenConfExec = "dbgen"
   ,dbGenConfTables = mkTbl <$> tblNames
   ,dbGenConfScale = 0.01
   ,dbGenConfIncremental = True
   ,dbGenConfSchema = fmap4 CppSymbol ssbSchema
  }
  where
    mkTbl :: String -> DBGenTableConf
    mkTbl [] = error "empty table name"
    mkTbl base@(c:_) =
      DBGenTableConf { dbGenTableConfChar = c,dbGenTableConfFileBase = base }
