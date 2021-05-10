{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE CPP        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TupleSections        #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module FluiDB.Schema.Graph.Main
  (graphMain) where

import FluiDB.Classes
import Data.Utils.Unsafe
import Data.Cluster.Types.Clusters
import Data.CnfQuery.BuildUtils
import FluiDB.Schema.Workload
import Data.Utils.Functors
import Data.Query.Algebra
import FluiDB.Schema.Graph.Schemata
import Data.NodeContainers
import Data.Codegen.Build
import Data.Utils.Tup
import Data.Utils.AShow
import           Data.Bifunctor
import           Data.List
import           Data.Maybe
import           GHC.Generics
import           Text.Printf

-- Workload stuff
on :: Integer -> (a -> a) -> [a] -> Maybe [a]
on _ _ []     = Nothing
on 0 f (x:xs) = Just $ f x:xs
on i f (x:xs) = (x:) <$> on (i-1) f xs


-- |variant 10 (1, False) [(a,b), (c,d)] --> [(a,b), (c,d + 10)]
variant :: Integer -> (Integer, Bool) -> [(Integer, Integer)] -> Maybe [(Integer, Integer)]
variant _ _ [] = Nothing
variant pert (index, onLeft) q = on index modifier q
  where
    modifier = (if onLeft then first else second) (+ (pert + topTable))
    topTable = foldl1 max $ (fst <$> q) ++ (snd <$> q)

workloadAlist :: [(String, QuerySize -> Workload)]
workloadAlist = [
  -- A star join, change a specific point in each iteration
  ("simple-star", \s -> starJoinVariants s $ repeat (1, False)),
  -- A star join, each time change a different point.
  ("alternating-star", \s -> starJoinVariants s $ cycle $ (,False) <$> [0..s-1])
  ]
  where
    starJoinVariants starSize =
      catMaybes
      . fmap ($ (1,) <$> [2..starSize + 1])
      . zipWith variant [1..]


-- MAIN
data EvalDesc q = F [q] | R [q] | D [q] deriving (Show, Generic)
instance (AShow [q], AShow q) => AShow (EvalDesc q)

evaluationDesc
  :: Evaluation e s t n q -> EvalDesc (q,Tup2 [NodeRef n],NodeRef n)
evaluationDesc = \case
  ForwardEval clust io q -> F [(q,io,snd $ primaryNRef clust)]
  ReverseEval clust io q -> R [(q,io,snd $ primaryNRef clust)]
  Delete n q -> D [(q,Tup2 [] [],n)]

instance MonadFail (Either String) where
  fail = Left

instance MonadFakeIO (Either String)

actualMain :: WorkloadConfig -> IO ()
actualMain wlConf = do
  putStrLn $ printf "Plans:"
  putStrLn $ ashow [(q,evaluationDesc <$> vs) | (q,vs) <- vals]
  where
    vals :: forall e s t n .
         (s ~ Integer,GraphTypeVars e s t n)
         => [([Integer],[(Evaluation e s t n SExp)])]
    vals =
      zip (nub <$> fmap (toInts =<<) queryVariations)
      $ fmap3 (showQ . cnfOrigDEBUG . head)
      $ fromRight
      $ runWorkloadEvals id queryVariations
      where
        fromRight = \case
          Left e -> error e
          Right r -> r
        -- budget=Just $ maybe 50 workloadBudget $ listToMaybe wlConfs
        queryVariations :: Workload
        queryVariations = mkWorkload wlConf
        showQ :: Query e s -> SExp
        showQ = \case
          J _ l r -> ashow' (toList l,toList r)
          q -> ashow' $ toList q
        toInts (a,b) = [a,b]


mkWorkload :: WorkloadConfig -> Workload
mkWorkload WorkloadConfig{..} =
  take workloadSize
  $ fmap2 ((+ workloadTableOffset) `bimap` (+ workloadTableOffset))
  $ ($ workloadQuerySize)
  $ fromJustErr
  $ workloadName `lookup` workloadAlist

type Length = Int
type QuerySize = Integer
type Workload = [[(Integer,Integer)]]
data WorkloadConfig = WorkloadConfig {
  workloadName          :: String
  , workloadTableOffset :: Integer
  , workloadQuerySize   :: QuerySize
  , workloadSize        :: Length
  , workloadBudget      :: Int
  } deriving Show

defaultConfig :: WorkloadConfig
defaultConfig = WorkloadConfig {
  workloadName="simple-star",
  workloadTableOffset=0,
  workloadQuerySize=3,
  workloadSize=20,
  workloadBudget=20
  }

graphMain :: IO ()
graphMain = actualMain defaultConfig
