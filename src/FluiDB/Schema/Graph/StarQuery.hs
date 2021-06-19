{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
module FluiDB.Schema.Graph.StarQuery
  (mkWorkload
  ,SovledStarQ(..)
  ,StarQ(..)
  ,WorkloadConf(..),defaultConfig) where

import           Data.Codegen.Build
import           Data.Proxy
import           Data.Utils.AShow
import           FluiDB.Classes
import           FluiDB.Runner.DefaultGlobal
import           FluiDB.Schema.Graph.Schemata
import           FluiDB.Schema.Graph.Values
import           GHC.Generics

listMod :: Int -> (a -> a) -> [a] -> [a]
listMod _ _ []     = []
listMod i f as0 = go i as0
  where
    go _ []     = []
    go 0 (a:as) = f a : as
    go i (a:as) = a : go (i - 1) as

starQMod :: Int -> (Int -> Int) -> StarQ -> StarQ
starQMod i f sq = sq{sqTs=listMod i f $ sqTs sq}

iniStarQ :: Int -> StarQ
iniStarQ size = StarQ { sqI = 1,sqTs = [2 .. size] }

-- |Generate a sequence of StarQueries by changing one join at index.
starQGen :: StarQueryIndex -> StarQ -> [StarQ]
starQGen chIndex ini = [setStarQ $ i + starQSize | i <- [0 ..]]
  where
    starQSize = length $ sqTs ini
    setStarQ i = starQMod  chIndex (const i) ini

mkWorkload :: QuerySize -> Workload
mkWorkload starSize = starQGen (starSize `div` 2) (iniStarQ starSize)


type StarQueryIndex = Int
type QuerySize = Int
type Workload = [StarQ]

data StarQ = StarQ { sqI :: Int,sqTs :: [Int] }
  deriving Generic
instance AShow StarQ
data SovledStarQ e s t n =
  QuerySol { qsQuery :: StarQ,qsSol :: [Evaluation e s t n SExp] }


data WorkloadConf =
  WorkloadConf
  { wcQuerySize :: QuerySize,wcWorkloadSize :: Int,wcBudget :: Int }

defaultConfig :: WorkloadConf
defaultConfig =
  WorkloadConf { wcQuerySize = 4,wcWorkloadSize = 10,wcBudget = 10 }


instance MonadFakeIO m => DefaultGlobal (Int,Int) Int () () m StarQ where
  defGlobalConf Proxy = graphGlobalConf . mkGraphSchema . (>>= starToEdges)
  putPS Proxy = putPS (Proxy :: Proxy [(Int,Int)])
  getIOQuery = getIOQuery  . starToEdges

starToEdges :: StarQ -> [(Int,Int)]
starToEdges StarQ {..} = fmap (sqI,) sqTs
