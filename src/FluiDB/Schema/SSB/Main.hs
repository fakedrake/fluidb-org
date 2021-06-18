module FluiDB.Schema.SSB.Main () where


import           Control.Monad.State
import           Data.Codegen.Build
import           Data.Codegen.Run
import qualified Data.IntMap               as IM
import           Data.Query.Algebra
import           Data.Query.SQL.Types
import           Data.Utils.AShow
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

annotateQuery
  :: QueryCppConf ExpTypeSym Table
  -> SSBQuery
  -> SSBGlobalSolveM AnnotQuery
annotateQuery = error "Not implemented"


actualMain :: [Int] -> IO ()
actualMain = mapM_ $ \qi -> runGlobalSolve ssbGlobalConf (fail . ashow) $ do
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
