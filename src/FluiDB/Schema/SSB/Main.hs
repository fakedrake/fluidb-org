module FluiDB.Schema.SSB.Main () where

import           Data.Query.Algebra
data SSBField
data SSBTbl
data SSBLit = SSBNum Int | SSBString String
data SSBFilter
  = Lt SSBField SSBField
  | ForeignEq SSBField SSBField
  | Rng SSBField SSBLit SSBLit
  | Or SSBFilter SSBFilter
data SSBQuery =
  SSBQuery
  { ssbqSel    :: [Aggr (Expr SSBField)]
   ,ssbqTbls   :: [SSBTbl]
   ,ssbqFilter :: [SSBFilter]
   ,ssbqGroup  :: [SSBField]
   ,ssbqSort   :: [SSBField]
  }

ssbToQ :: SSBQuery -> Query SSBESym SSBTbl
ssbToQ = _

actualMain :: [SSBQuery] -> IO ()
actualMain wlConf = do
  putStrLn $ printf "Plans:"
  putStrLn $ ashow [(q,evaluationDesc <$> vs) | QuerySol q vs <- vals]
  where
    vals :: forall e s t n .
         (s ~ Int,GraphTypeVars e s t n)
         => [SovledStarQ e s t n]
    vals =
      zipWith QuerySol workload
      $ fmap3 cnfSexp
      $ fromRight
      $ runWorkloadEvals
        (\conf -> conf
         { globalGCConfig = (globalGCConfig conf)
             { budget = Just $ wcBudget wlConf }
         })
        workload
      where
        cnfSexp = showQ . cnfOrigDEBUG . head
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
graphMain = timeout 3000000 (actualMain []) >>= \case
  Nothing -> putStrLn "TIMEOUT!!"
  Just () -> putStrLn "Done!"
