{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-deferred-type-errors #-}
module FluiDB.Schema.SSB.Main (graphMain) where


import           Control.Monad.Except
import           Control.Monad.State
import           Data.Bipartite
import           Data.Codegen.Build
import           Data.Codegen.Run
import           Data.DotLatex
import qualified Data.IntMap               as IM
import           Data.NodeContainers
import           Data.Query.Algebra
import           Data.Query.SQL.Types
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Ranges
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


finallyError :: MonadError e m => m a -> m () -> m a
finallyError m hndl = do
  ret <- m `catchError` (\e -> hndl >> throwError e)
  hndl
  return ret

runQuery :: SSBQuery -> SSBGlobalSolveM ()
runQuery query = do
  cppConf <- gets globalQueryCppConf
  aquery <- annotateQuerySSB cppConf query
  (transitions,_cppCode) <- finallyError (runSingleQuery aquery) $ do
    (queryPath,pngPath,grPath) <- renderGraph query
    liftIO $ putStrLn $ "Inspect the query at: " ++ queryPath
    liftIO $ putStrLn $ "Inspect the graph at: " ++ pngPath
    liftIO $ putStrLn $ "The raw graph at: " ++ grPath
  -- liftIO $ runCpp cppCode
  liftIO $ putStrLn $ ashow transitions
  return ()

ssbRunGlobalSolve :: SSBGlobalSolveM a -> IO a
ssbRunGlobalSolve m = do
  ssbGlobalConf <- getSsbGlobalConf
  runGlobalSolve ssbGlobalConf (fail . ashow) m

singleQuery :: IO ()
singleQuery =
  ssbRunGlobalSolve
  $ runQuery
  $ ssbParse
  $ unwords
    ["select c_city, s_city, d_year, sum(lo_revenue) as revenue"
    ,"from customer, lineorder, supplier, date"
    ,"where lo_custkey = c_custkey"
    ,"and lo_suppkey = s_suppkey"
    ,"and lo_orderdate = d_datekey"
    ,"and c_nation = 'UNITED STATES'" -- this
    ,"and s_nation = 'UNITED STATES'" -- this
    ,"and d_year >= 1992 and d_year <= 1997" -- this
    ,"group by c_city, s_city, d_year"
    ,"order by d_year, revenue desc"]
-- XXX: removing any of the "this" lines makes singleQuery run.


actualMain :: [Int] -> IO ()
actualMain qs = ssbRunGlobalSolve $ forM_ qs $ \qi -> do
  liftIO $ putStrLn $ "Running query: " ++ show qi
  case IM.lookup qi ssbQueriesMap of
    Nothing    -> throwAStr $ printf "No such query %d" qi
    Just query -> runQuery query


type ImagePath = FilePath
type QueryPath = FilePath
type GraphPath = FilePath
renderGraph :: SSBQuery -> SSBGlobalSolveM (QueryPath, ImagePath,GraphPath)
renderGraph query = do
  gr <- gets $ propNet . getGlobalConf
  liftIO $ tmpDir' KeepDir "graph_render" $ \d -> do
    let graphBase = d </> "graph"
        dotPath = graphBase <.> "dot"
        qPath = graphBase <.> "query"
        grPath = graphBase <.> "graph"
        imgPath = graphBase <.> "png"
    writeFile grPath $ ashow gr
    writeFile qPath $ ashow query
    writeFile dotPath $ simpleRender @T @N gr
    withFile dotPath ReadMode $ \hndl -> do
      runProc
        (mkProc "dot" ["-o" ++ imgPath, "-Tpng"]) { std_in = UseHandle hndl }
      return (qPath,imgPath,grPath)

getInputNodes :: NodeRef N -> GraphBuilderT T N IO [(NodeRef T,[NodeRef N])]
getInputNodes ref = do
  ts <- getNodeLinksN
    NodeLinksFilter { nlfSide = [Inp],nlfIsRev = fullRange,nlfNode = ref }
    >>= maybe (fail $ "No such n-node: " ++ show ref) return
  forM (toNodeList ts) $ \t -> do
    ns <- getNodeLinksT
      NodeLinksFilter { nlfSide = [Inp],nlfIsRev = fullRange,nlfNode = t }
      >>= maybe (fail $ "No such t-node: " ++ show t) return
    return (t,toNodeList ns)

readGraph :: GraphPath -> GraphBuilderT T N IO a -> IO a
readGraph gpath m = do
  gr <- aread <$> readFile gpath
  case gr of
    Nothing -> fail $ "Failed to read graph at: " ++ gpath
    Just g  -> evalStateT m def{gbPropNet = g}

graphMain :: IO ()
graphMain = timeout 3000000 (actualMain [1..12]) >>= \case
  Nothing -> putStrLn "TIMEOUT!!"
  Just () -> putStrLn "Done!"
