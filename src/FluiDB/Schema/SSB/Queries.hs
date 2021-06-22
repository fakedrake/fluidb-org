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

import           Data.CppAst.CodeSymbol
import           Data.CppAst.CppType
import           Data.Foldable
import qualified Data.IntMap                  as IM
import           Data.List
import           Data.Query.Algebra
import           Data.Query.QuerySchema.Types
import           Data.Query.SQL.Parser
import           Data.Query.SQL.Types
import           Data.Utils.Functors
import           FluiDB.Bamify.Types

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
  "date"      -> "d_"
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
ssbSchema =
  [("lineorder",lineorderSchema)
  ,("date",dateSchema)
  ,("part",partSchema)
  ,("supplier",supplierSchema)
  ,("customer",customerSchema)]
  where
    customerSchema =
      -- long            custkey;
      [(CppNat,"c_custkey")
       -- char            name[C_NAME_LEN + 1];
      ,(strType 18,"c_name")
       -- char            address[C_ADDR_MAX + 1];
      ,(strType 40,"c_address")
       -- char            city[CITY_FIX+1];
      ,(strType 11,"c_city")
       -- char            nation_name[C_NATION_NAME_LEN+1];
      ,(strType 15,"c_nation_name")
       -- char            region_name[C_REGION_NAME_LEN+1];
      ,(strType 13,"c_region_name")
       -- char            phone[PHONE_LEN + 1];
      ,(strType 15,"c_phone")
       -- char            mktsegment[MAXAGG_LEN + 1];
      ,(strType 21,"c_mktsegment")]
    supplierSchema =
      -- long            suppkey;
      [(CppNat,"s_suppkey")
       -- char            name[S_NAME_LEN + 1];
      ,(strType 25,"s_name")
       -- char            address[S_ADDR_MAX + 1];
      ,(strType 40,"s_address")
       -- char            city[CITY_FIX +1];
      ,(strType 16,"s_city")
       -- char            nation_name[S_NATION_NAME_LEN+1];
      ,(strType 16,"s_nation_name")
       -- char            region_name[S_REGION_NAME_LEN+1];
      ,(strType 13,"s_region_name")
       -- char            phone[PHONE_LEN + 1];
      ,(strType 15,"s_phone")]
    partSchema =
      -- long           partkey;
      [(CppNat,"p_partkey")
       -- char           name[P_NAME_LEN + 1];
      ,(strType 55,"p_name")
       -- char           mfgr[P_MFG_LEN + 1];
      ,(strType 25,"p_mfgr")
       -- char           category[P_CAT_LEN + 1];
      ,(strType 7,"p_category")
       -- char           brand[P_BRND_LEN + 1];
      ,(strType 10,"p_brand")
       -- char           color[P_COLOR_MAX + 1];
      ,(strType 11,"p_color")
       -- char           type[P_TYPE_MAX + 1];
      ,(strType 25,"p_type")
       -- long            size;
      ,(CppNat,"p_size")
       -- char           container[P_CNTR_LEN + 1];
      ,(strType 10,"p_container")]
    lineorderSchema =
      [(CppNat,"lo_orderkey")
       -- int             linenumber; /*integer, constrain to max of 7*/
      ,(CppInt,"lo_linenumber")
       -- long            custkey;
      ,(CppNat,"lo_custkey")
       -- long            partkey;
      ,(CppNat,"lo_partkey")
       -- long            suppkey;
      ,(CppNat,"lo_suppkey")
       -- char            orderdate[DATE_LEN];
      ,(dateType,"lo_orderdate")
       -- char            opriority[MAXAGG_LEN + 1];
      ,(strType 21,"lo_opriority")
       -- long            ship_priority;
      ,(CppNat,"lo_ship_priority")
       -- char            shipmode[O_SHIP_MODE_LEN + 1];
      ,(strType 10,"lo_shipmode")
       -- long             quantity;
      ,(CppNat,"lo_quantity")
       -- long           extended_price;
      ,(CppDouble,"lo_extendedprice")
       -- long           order_totalprice;
      ,(CppNat,"lo_orderstatus")
       -- long           discount;
      ,(CppDouble,"lo_discount")
       -- long           revenue;
      ,(CppNat,"lo_revenue")
       -- long           supp_cost;
      ,(CppNat,"lo_supp_cost")
       -- long           tax;
      ,(CppDouble,"lo_tax")
       -- char            commit_date[DATE_LEN] ;
      ,(strType 13,"lo_commitmedium")]
    dateSchema =
      [(CppNat,"d_datekey")
      ,(strType 18,"d_date")
      ,(strType 9,"d_dayofweek")
      ,(strType 9,"d_month")
      ,(CppNat,"d_year")
      ,(CppNat,"d_yearmonthnum")
      ,(strType 7,"d_yearmonth")
      ,(CppNat,"d_daynuminweek")
      ,(CppNat,"d_daynuminmonth")
      ,(CppNat,"d_daynuminyear")
      ,(CppNat,"d_monthnuminyear")
      ,(CppNat,"d_weeknuminyear")
      ,(strType 15,"d_sellingseason")
      ,(strType 2,"d_lastdayinweekfl")
      ,(strType 2,"d_lastdayinmonthfl")
      ,(strType 2,"d_holidayfl")
      ,(strType 2,"d_weekdayfl")]

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
     ,"from lineorder, date"
     ,"where lo_orderdate = d_datekey"
     ,"and d_yearmonthnum = 199401"
     ,"and lo_discount between 4 and 6"
     ,"and lo_quantity between 26 and 35"]
    ,["select sum(lo_extendedprice*lo_discount) as revenue"
     ,"from lineorder, date"
     ,"where lo_orderdate = d_datekey"
     ,"and d_year = 1993"
     ,"and lo_discount between 1 and 3"
     ,"and lo_quantity < 25"]
    ,["select sum(lo_extendedprice*lo_discount) as revenue"
     ,"from lineorder, date"
     ,"where lo_orderdate = d_datekey"
     ,"and d_yearmonthnum = 199401"
     ,"and lo_discount between 4 and 6"
     ,"and lo_quantity between 26 and 35"]
    ,["select sum(lo_extendedprice*lo_discount) as revenue"
     ,"from lineorder, date"
     ,"where lo_orderdate = d_datekey"
     ,"and d_weeknuminyear = 6"
     ,"and d_year = 1994"
     ,"and lo_discount between 5 and 7"
     ,"and lo_quantity between 26 and 35"]
    ,["select sum(lo_revenue), d_year, p_brand1"
     ,"from lineorder, date, part, supplier"
     ,"where lo_orderdate = d_datekey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and p_category = 'MFGR#12'"
     ,"and s_region = 'AMERICA'"
     ,"group by d_year, p_brand1"
     ,"order by d_year, p_brand1"]
    ,["select sum(lo_revenue), d_year, p_brand1"
     ,"from lineorder, date, part, supplier"
     ,"where lo_orderdate = d_datekey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and p_brand1 between 'MFGR#2221' and 'MFGR#2228'"
     ,"and s_region = 'ASIA'"
     ,"group by d_year, p_brand1"
     ,"order by d_year, p_brand1"]
    ,["select sum(lo_revenue), d_year, p_brand1"
     ,"from lineorder, date, part, supplier"
     ,"where lo_orderdate = d_datekey"
     ,"and lo_partkey = p_partkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and p_brand1 = 'MFGR#2221'"
     ,"and s_region = 'EUROPE'"
     ,"group by d_year, p_brand1"
     ,"order by d_year, p_brand1"]
    ,["select c_nation, s_nation, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, date"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and c_region = 'ASIA' and s_region = 'ASIA'"
     ,"and d_year >= 1992 and d_year <= 1997"
     ,"group by c_nation, s_nation, d_year"
     ,"order by d_year, revenue desc"]
    ,["select c_city, s_city, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, date"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and c_nation = 'UNITED STATES'"
     ,"and s_nation = 'UNITED STATES'"
     ,"and d_year >= 1992 and d_year <= 1997"
     ,"group by c_city, s_city, d_year"
     ,"order by d_year, revenue desc"]
    ,["select c_city, s_city, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, date"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and (c_city='UNITED KI1' or c_city='UNITED KI5')"
     ,"and (s_city='UNITED KI1' or s_city='UNITED KI5')"
     ,"and d_year >= 1992 and d_year <= 1997"
     ,"group by c_city, s_city, d_year"
     ,"order by d_year, revenue desc"]
    ,["select c_city, s_city, d_year, sum(lo_revenue) as revenue"
     ,"from customer, lineorder, supplier, date"
     ,"where lo_custkey = c_custkey"
     ,"and lo_suppkey = s_suppkey"
     ,"and lo_orderdate = d_datekey"
     ,"and (c_city='UNITED KI1' or c_city='UNITED KI5')"
     ,"and (s_city='UNITED KI1' or s_city='UNITED KI5')"
     ,"and d_yearmonth = 'Dec1997'"
     ,"group by c_city, s_city, d_year"
     ,"order by d_year, revenue desc"]
    ,["select d_year, c_nation, sum(lo_revenue - lo_supplycost) as profit"
     ,"from date, customer, supplier, part, lineorder"
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
     ,"from date, customer, supplier, part, lineorder"
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
     ,"from date, customer, supplier, part, lineorder"
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


-- To build the ssb queries in the directory /tmp/fluidb-data:
-- ssbDBGen "/tmp/fluidb-data"
ssbTpchDBGenConf :: [SSBTable] -> DBGenConf
ssbTpchDBGenConf tblNames =
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
