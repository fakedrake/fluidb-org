module FluiDB.Schema.SSB.Main () where


import           Control.Monad.State
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.CnfQuery.Build
import           Data.Codegen.Build
import           Data.Codegen.Run
import           Data.Codegen.Schema
import qualified Data.IntMap                           as IM
import           Data.Query.Algebra
import           Data.Query.Optimizations.ExposeUnique
import           Data.Query.QuerySchema
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Functors
import           Data.Utils.ListT
import           Data.Utils.Unsafe
import           FluiDB.Schema.SSB.Queries
import           FluiDB.Schema.SSB.Values
import           FluiDB.Schema.Workload
import           FluiDB.Types
import           FluiDB.Utils
import           System.Timeout
import           Text.Printf

type AnnotQuery =
  Query (PlanSym ExpTypeSym Table) (QueryPlan ExpTypeSym Table,Table)
type SSBQuery = Query ExpTypeSym Table
type SSBGlobalSolveM = GlobalSolveT ExpTypeSym Table T N IO

putPlanSymTpch
  :: QueryCppConf ExpTypeSym Table
  -> SSBQuery
  -> Either (GlobalError e s t n) (AnnotQuery)
putPlanSymTpch cppConf q = do
  let uniqSym = \e -> do
        i <- get
        case asUnique cppConf i e of
          Just e' -> modify (+ 1) >> return e'
          Nothing -> throwAStr $ "Not a symbol: " ++ ashow e
  qUniqExposed <- maybe
    (throwAStr "Couldn't find uniq:")
    ((>>= maybe (throwAStr "Couldn't expose uniques") (return . fst))
     . headListT
     . (`evalStateT` (0 :: Int))
     . exposeUnique uniqSym)
    $ traverse (\s -> (s,) <$> uniqueColumns cppConf s) q
  first toGlobalError
    $ (>>= maybe (throwAStr "Unknown symbol") return)
    $ fmap
      (bitraverse
         (pure . uncurry mkPlanSym)
         (\s -> (,s) <$> mkPlanFromTbl cppConf s)
       . snd
       . fromJustErr)
    $ (`evalStateT` def)
    $ headListT
    $ toCNF (fmap2 snd . tableSchema cppConf) qUniqExposed


annotateQuery
  :: QueryCppConf ExpTypeSym Table
  -> SSBQuery
  -> SSBGlobalSolveM AnnotQuery
annotateQuery cppConf query = _ $ putPlanSymTpch cppConf query


actualMain :: [Int] -> IO ()
actualMain qs = do
  ssbGlobalConf <- getSsbGlobalConf
  forM_  qs $ \qi -> runGlobalSolve ssbGlobalConf (fail . ashow) $ do
  case IM.lookup qi ssbQueriesMap of
    Nothing -> throwAStr $ printf "No such query %d" qi
    Just query -> do
      cppConf <- gets globalQueryCppConf
      aquery <- annotateQuery cppConf query
      (_transitions,cppCode)  <- runSingleQuery aquery
      liftIO $ runCpp cppCode

graphMain :: IO ()
graphMain = timeout 3000000 (actualMain []) >>= \case
  Nothing -> putStrLn "TIMEOUT!!"
  Just () -> putStrLn "Done!"
