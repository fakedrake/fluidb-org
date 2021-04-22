{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}

module FluiDB.Schema.TPCH.Main
  (tpchMain) where

import FluiDB.Schema.Workload
import FluiDB.Runner.DefaultGlobal
import Data.QueryPlan.CostTypes
import Data.Utils.AShow
import Data.QueryPlan.Types
import FluiDB.Types
import System.Exit
import Data.NodeContainers
import Control.Monad.Except
import FluiDB.Classes
import Data.Proxy
import           Control.Monad
import           System.Timeout
import           Text.Printf

timeoutSecs :: Int -> IO () -> IO ()
timeoutSecs t m = timeout (t * 1000000) m >>= \case
  Nothing -> fail $ "Timed out (" ++ show t ++ "s)"
  Just () -> return ()

workloadStar :: [[(Integer,Integer)]]
workloadStar = [[(1,2),(1,i),(1,3)] | i <- [4..10]]

workloadsTpchSeq :: [Int]
workloadsTpchSeq = [1..22]
workloadsTpchCycle :: [Int]
workloadsTpchCycle = join [(+i) <$> [1,2,3,4] | i <- [0..10]]
repeatTpch :: Int -> [Int]
repeatTpch x = take 10 $ repeat x
workloadTpch :: [Int]
workloadTpch = [7,8,7,8,7,8,7,8]

runWorkload' :: (Int -> Int) -> _ -> IO (Either String _)
runWorkload' f wl = do
  let modBudget gc = gc{
        globalGCConfig=(globalGCConfig gc){
            budget=fmap f $ budget $ globalGCConfig gc}}
  logMsg "Schema:"
  logMsg $ ashow $ globalSchemaAssoc $ defGlobalConf (Proxy :: Proxy IO) wl
  logMsg $ "Running workload: " ++ show wl
  runExceptT $ runWorkload modBudget wl

tpchMain :: IO ()
tpchMain = timeoutSecs 15 $ do
  let workload = workloadTpch
  runWorkload' id workload >>= \case
    Left e -> die e
    Right results -> do
      putStrLn "Results:"
      costs <- forM (zip workload results) $ \(i,(ts,deps)) -> do
        let cost@(Cost r w) = mconcat $ snd <$> ts
        putStrLn $ printf "%sg: %8d/%8d -> %s" (show i) r w (show deps)
        return cost
      putStrLn $ printf "Total cost: %s" $ ashow $ mconcat costs


-- Sequential
-- Results:
--      467 -> [<2>]
--  4182283 -> [<3>,<5>,<6>,<7>,<8>]
--    49753 -> [<1>,<2>,<4>]
--      130 -> [<2>,<4>]
--   231559 -> [<1>,<2>,<3>,<4>,<7>,<8>]
--      269 -> [<2>]
--    10040 -> [<1>,<2>,<3>,<4>,<8>]
--    44263 -> [<1>,<2>,<3>,<4>,<5>,<7>,<8>]
--    28399 -> [<2>,<4>,<5>,<6>,<31>]
--    82653 -> [<1>,<2>,<3>,<4>]
--  1906942 -> [<3>,<6>,<8>]
--      779 -> [<2>,<4>]
--      534 -> [<1>,<4>]
--     1718 -> [<2>,<5>]
--     1697 -> [<2>,<8>]
--   757655 -> [<5>,<6>,<8>]
--     8162 -> [<5>]
--   125737 -> [<2>,<283>]
--     7232 -> [<2>,<5>]
--    90472 -> [<2>,<3>,<5>,<6>,<8>]
--     3393 -> [<2>,<3>,<4>,<8>]
--     1086 -> [<1>,<4>]
-- Total cost: 7535223

-- Individual
-- Results:
--      467 -> [<2>]
--  4182283 -> [<3>,<5>,<6>,<7>,<8>]
--    49753 -> [<1>,<2>,<4>]
--      130 -> [<2>,<4>]
--   231559 -> [<1>,<2>,<3>,<4>,<7>,<8>]
--      269 -> [<2>]
--    10040 -> [<1>,<2>,<3>,<4>,<8>]
--   204548 -> [<1>,<2>,<3>,<4>,<5>,<7>,<8>]
--    29069 -> [<2>,<3>,<4>,<5>,<6>,<8>]
--    82653 -> [<1>,<2>,<3>,<4>]
--  1906942 -> [<3>,<6>,<8>]
--      779 -> [<2>,<4>]
--      534 -> [<1>,<4>]
--     1718 -> [<2>,<5>]
--     1697 -> [<2>,<8>]
--   757655 -> [<5>,<6>,<8>]
--     8162 -> [<5>]
--   125849 -> [<1>,<2>,<4>]
--     7232 -> [<2>,<5>]
--    90472 -> [<2>,<3>,<5>,<6>,<8>]
--     3393 -> [<2>,<3>,<4>,<8>]
--     1086 -> [<1>,<4>]
-- Total cost: 7696290
