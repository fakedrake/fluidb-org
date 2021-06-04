{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}

module FluiDB.Schema.TPCH.Main
  (tpchMain) where

import Data.NodeContainers
import FluiDB.Schema.Workload
import FluiDB.Runner.DefaultGlobal
import Data.QueryPlan.CostTypes
import Data.Utils.AShow
import Data.QueryPlan.Types
import FluiDB.Types
import System.Exit
import Control.Monad.Except
import FluiDB.Classes
import Data.Proxy
import           System.Timeout
import           Text.Printf
import Prelude hiding (log)

timeoutSecs :: Int -> IO () -> IO ()
timeoutSecs t m = timeout (t * 1000000) m >>= \case
  Nothing -> fail $ "Timed out (" ++ show t ++ "s)"
  Just () -> return ()

workloadTpch :: [Int]
workloadTpch = [7,8,7,8,7,8,7,8]

runWorkload'
  :: Maybe Int
  -> [Int]
  -> IO (Either String [([(Transition () (),Cost)],[NodeRef ()])])
runWorkload' budget wl = do
  let modBudget gc =
        gc { globalGCConfig = (globalGCConfig gc) { budget = budget } }
  let log = ioLogMsg ioOps
  log "Schema:"
  log $ ashow $ globalSchemaAssoc $ defGlobalConf (Proxy :: Proxy IO) wl
  log $ "Running workload: " ++ show wl
  runExceptT $ runWorkloadCpp modBudget wl

tpchMain :: IO ()
tpchMain = timeoutSecs 15 $ do
  let workload = workloadTpch
  runWorkload' (Just 100) workload >>= \case
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
