{-# LANGUAGE CPP                  #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
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

import           Data.Cluster.Types
import           Data.QnfQuery.BuildUtils
import           Data.Codegen.Build
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Tup
import           FluiDB.Classes
import           FluiDB.Schema.Graph.Schemata
import           FluiDB.Schema.Graph.StarQuery
import           FluiDB.Schema.Workload
import           FluiDB.Types
import           GHC.Generics
import           System.Timeout
import           Text.Printf


-- MAIN
data EvalDesc q = F [q] | R [q] | D [q] deriving (Show, Generic)
instance (AShow [q], AShow q) => AShow (EvalDesc q)

evaluationDesc
  :: Evaluation e s t n q -> EvalDesc (q,Tup2 [NodeRef n],NodeRef n)
evaluationDesc = \case
  ForwardEval clust io q -> F [(q,io,snd $ primaryNRef clust)]
  ReverseEval clust io q -> R [(q,io,snd $ primaryNRef clust)]
  Delete n q             -> D [(q,Tup2 [] [],n)]

instance MonadFail (Either String) where
  fail = Left

instance MonadFakeIO (Either String)

actualMain :: WorkloadConf -> IO ()
actualMain wlConf = do
  putStrLn $ printf "Plans:"
  putStrLn $ ashow [(q,evaluationDesc <$> vs) | QuerySol q vs <- vals]
  where
    vals :: forall e s t n .
         (s ~ Int,GraphTypeVars e s t n)
         => [SovledStarQ e s t n]
    vals =
      zipWith QuerySol workload
      $ fmap3 qnfSexp
      $ fromRight
      $ runWorkloadEvals
        (\conf -> conf
         { globalGCConfig = (globalGCConfig conf)
             { budget = Just $ wcBudget wlConf }
         })
        workload
      where
        qnfSexp = showQ . qnfOrigDEBUG . head
        fromRight = \case
          Left e  -> error e
          Right r -> r
        workload :: [StarQ]
        workload =
          take (wcWorkloadSize wlConf) $ mkWorkload $ wcQuerySize wlConf
        showQ :: Query e s -> SExp
        showQ = \case
          J _ l r -> ashow' (toList l,toList r)
          q       -> ashow' $ toList q

graphMain :: IO ()
graphMain =
  timeout 3000000 (actualMain defaultConfig) >>= \case
  Nothing -> putStrLn "TIMEOUT!!"
  Just () -> putStrLn "Done!"
