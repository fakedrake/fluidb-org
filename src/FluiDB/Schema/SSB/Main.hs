{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-deferred-type-errors #-}
module FluiDB.Schema.SSB.Main (ssbMain) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.State
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build
import           Data.Codegen.Run
import           Data.DotLatex
import qualified Data.IntMap               as IM
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.SQL.Types
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Functors
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

type AnnotQuery =
  Query (ShapeSym ExpTypeSym Table) (QueryShape ExpTypeSym Table,Table)
type SSBQuery = Query ExpTypeSym Table
type SSBGlobalSolveM = GlobalSolveT ExpTypeSym Table T N IO


annotateQuerySSB
  :: QueryCppConf ExpTypeSym Table -> SSBQuery -> SSBGlobalSolveM AnnotQuery
annotateQuerySSB cppConf query = do
  symSizeAssoc <- gets globalTableSizeAssoc
  let luSize s = lookup s symSizeAssoc
  either throwError return $ annotateQuery cppConf luSize query


finallyError :: MonadError e m => m a -> m () -> m a
finallyError m hndl = do
  ret <- m `catchError` (\e -> hndl >> throwError e)
  hndl
  return ret

shouldRender :: Verbosity -> Bool
shouldRender Verbose = True
shouldRender Quiet   = False

runQuery :: Verbosity -> SSBQuery -> SSBGlobalSolveM [Transition T N]
runQuery verbosity  query = do
  cppConf <- gets globalQueryCppConf
  aquery <- annotateQuerySSB cppConf query
  (transitions,_cppCode)
    <- finallyError (runSingleQuery aquery) $ when (shouldRender verbosity) $ do
      (intermPath, queryPath,pngPath,grPath) <- renderGraph query
      liftIO $ putStrLn $ "Inspect the query at: " ++ queryPath
      liftIO $ putStrLn $ "Inspect the graph at: " ++ pngPath
      liftIO $ putStrLn $ "The raw graph at: " ++ grPath
      liftIO $ putStrLn $ "The reperoire of intermediates: " ++ intermPath
  -- liftIO $ runCpp cppCode
  liftIO $ putStrLn $ ashow transitions
  return transitions

softFail :: AShow a => a -> IO ()
softFail a = putStrLn $ ashow a

ssbRunGlobalSolve :: SSBGlobalSolveM a -> IO ()
ssbRunGlobalSolve m = do
  ssbGlobalConf <- getSsbGlobalConf
  runGlobalSolve ssbGlobalConf (softFail . ashow) $ void m

#ifdef EXAMPLE
singleQuery :: IO ()
singleQuery =
  void
  $ ssbRunGlobalSolve
  $ runQuery Verbose
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
#endif

data Verbosity = Verbose | Quiet
actualMain :: Verbosity -> [Int] -> IO ()
actualMain verbosity qs = ssbRunGlobalSolve $ forM_ qs $ \qi -> do
  mats <- globalizePlanT $ nodesInState [Initial Mat,Concrete NoMat Mat, Concrete Mat Mat]
  lift2 $ putStrLn $ "mat nodes: " ++ ashow mats
  liftIO $ putStrLn $ "Running query: " ++ show qi
  case IM.lookup qi ssbQueriesMap of
    Nothing -> throwAStr $ printf "No such query %d" qi
    Just query -> do
      _transitions <- runQuery verbosity query
      pgs <- globalizePlanT getDataSize
      lift2 $ putStrLn $ "Pages used: " ++ show pgs

type ImagePath = FilePath
type QueryPath = FilePath
type GraphPath = FilePath
type IntermediatesPath = FilePath


renderGraph
  :: SSBQuery
  -> SSBGlobalSolveM (IntermediatesPath,QueryPath,ImagePath,GraphPath)
renderGraph query = do
  gr <- gets $ propNet . globalGCConfig
  interms <- gets
    $ fmap3 (runIdentity . qnfOrigDEBUG')
    . refAssocs
    . nrefToQnfs
    . globalClusterConfig
  liftIO $ tmpDir' KeepDir "graph_render" $ \d -> do
    let graphBase = d </> "graph"
        dotPath = graphBase <.> "dot"
        qPath = graphBase <.> "query"
        grPath = graphBase <.> "graph"
        isPath = graphBase <.> "interm"
        imgPath = graphBase <.> "svg"
    writeFile grPath $ ashow gr
    writeFile qPath $ ashow query
    writeFile isPath $ ashow interms
    writeFile dotPath $ simpleRender gr
    withFile dotPath ReadMode $ \hndl -> do
      runProc
        (mkProc "dot" ["-o" ++ imgPath,"-Tsvg"]) { std_in = UseHandle hndl }
    return (isPath,qPath,imgPath,grPath)

#ifdef EXAMPLE
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
#endif

ssbMain :: IO ()
ssbMain = do
  -- let oneGig = ResourceLimit 1000000000
  -- setResourceLimit ResourceDataSize (ResourceLimits oneGig oneGig)
  let secs = 60
  traceTM "Starting!"
  timeout (secs * 1000000) (actualMain Verbose [1..6]) >>= \case
    Nothing -> putStrLn $ printf  "TIMEOUT after %ds" secs
    Just () -> putStrLn "Done!"
