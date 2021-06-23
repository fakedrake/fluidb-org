module FluiDB.Schema.SSB.Main (graphMain) where


import           Control.Monad.Except
import           Control.Monad.State
import           Data.Codegen.Build
import           Data.Codegen.Run
import qualified Data.IntMap               as IM
import           Data.Query.Algebra
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           FluiDB.Schema.Common
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


annotateQuerySSB
  :: QueryCppConf ExpTypeSym Table -> SSBQuery -> SSBGlobalSolveM AnnotQuery
annotateQuerySSB cppConf query =
  either throwError return $ annotateQuery cppConf query


actualMain :: [Int] -> IO ()
actualMain qs = do
  ssbGlobalConf <- getSsbGlobalConf
  forM_  qs $ \qi -> runGlobalSolve ssbGlobalConf (fail . ashow) $ do
  case IM.lookup qi ssbQueriesMap of
    Nothing -> throwAStr $ printf "No such query %d" qi
    Just query -> do
      cppConf <- gets globalQueryCppConf
      aquery <- annotateQuerySSB cppConf query
      (_transitions,cppCode)  <- runSingleQuery aquery
      liftIO $ runCpp cppCode

graphMain :: IO ()
graphMain = timeout 3000000 (actualMain [1..12]) >>= \case
  Nothing -> putStrLn "TIMEOUT!!"
  Just () -> putStrLn "Done!"
