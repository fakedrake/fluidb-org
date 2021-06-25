{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-deferred-type-errors #-}
module FluiDB.Schema.SSB.Main (graphMain) where


import           Control.Monad.Except
import           Control.Monad.State
import           Data.Codegen.Build
import           Data.Codegen.Run
import           Data.DotLatex
import qualified Data.IntMap               as IM
import           Data.Query.Algebra
import           Data.Query.SQL.Types
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           FluiDB.Schema.Common
import           FluiDB.Schema.SSB.Queries
import           FluiDB.Schema.SSB.Values
import           FluiDB.Schema.Workload
import           FluiDB.Types
import           FluiDB.Utils
import           System.FilePath
import           System.IO
import           System.Process
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
  runGlobalSolve ssbGlobalConf (fail . ashow) $ forM_ qs $ \qi -> do
    liftIO $ putStrLn $ "Running query: " ++ show qi
    case IM.lookup qi ssbQueriesMap of
      Nothing -> throwAStr $ printf "No such query %d" qi
      Just query -> do
        cppConf <- gets globalQueryCppConf
        aquery <- annotateQuerySSB cppConf query
        (transitions,_cppCode) <- catchError (runSingleQuery aquery) $ \e -> do
          (queryPath,pngPath) <- renderGraph query
          liftIO $ putStrLn $ "Inspect the query at: " ++ queryPath
          liftIO $ putStrLn $ "Inspect the graph at: " ++ pngPath
          throwError e
        -- liftIO $ runCpp cppCode
        liftIO $ putStrLn $ ashow transitions
        return ()

type ImagePath = FilePath
type QueryPath = FilePath
renderGraph :: SSBQuery -> SSBGlobalSolveM (QueryPath, ImagePath)
renderGraph query = do
  gr <- gets $ propNet . getGlobalConf
  liftIO $ tmpDir' KeepDir "graph_render" $ \d -> do
    let graphBase = d </> "graph"
        dotPath = graphBase <.> "dot"
        qPath = graphBase <.> "query"
        imgPath = graphBase <.> "png"
    writeFile qPath $ ashow query
    writeFile dotPath $ simpleRender @T @N gr
    withFile dotPath ReadMode $ \hndl -> do
      runProc
        (mkProc "dot" ["-o" ++ imgPath, "-Tpng"]) { std_in = UseHandle hndl }
      return (qPath,imgPath)

graphMain :: IO ()
graphMain = timeout 3000000 (actualMain [1..12]) >>= \case
  Nothing -> putStrLn "TIMEOUT!!"
  Just () -> putStrLn "Done!"
