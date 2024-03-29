{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-deferred-type-errors #-}
module FluiDB.Schema.SSB.Main (ssbMainWorkload, ssbMainIndiv) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.State
import           Data.Cluster.Types.Monad
import           Data.Codegen.Build
import           Data.Codegen.Run
import           Data.DotLatex
import           Data.IORef
import qualified Data.IntMap               as IM
import           Data.NodeContainers
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.QuerySize
import           Data.Query.SQL.Types
import           Data.QueryPlan.Nodes
import           Data.QueryPlan.Types
import           Data.Time
import           Data.Utils.AShow
import           Data.Utils.Debug
import           Data.Utils.Functors
import           Data.Utils.MTL
import           FluiDB.Schema.Common
import           FluiDB.Schema.SSB.Queries
import           FluiDB.Schema.SSB.Values
import           FluiDB.Schema.Workload
import           FluiDB.Types
import           FluiDB.Utils
import           System.Directory
import           System.FilePath
import           System.IO
import           System.IO.Unsafe
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

type WLabel  = String
runQuery
  :: WLabel
  -> Verbosity
  -> WIndex
  -> SSBQuery
  -> SSBGlobalSolveM [Transition T N]
runQuery lbl verbosity windex query = do
  mem <- gets $ budget . globalGCConfig
  cppConf <- gets globalQueryCppConf
  aquery <- annotateQuerySSB cppConf query
  (ref,transitions,cppCode)
    <- finallyError (runSingleQuery aquery) $ when (shouldRender verbosity) $ do
      (intermPath,queryPath,pngPath,grPath) <- renderGraph query
      liftIO $ putStrLn $ "Inspect the query at: " ++ queryPath
      liftIO $ putStrLn $ "Inspect the graph at: " ++ pngPath
      liftIO $ putStrLn $ "The raw graph at: " ++ grPath
      liftIO $ putStrLn $ "The reperoire of intermediates: " ++ intermPath
  -- liftIO $ runCpp cppCode
  let planFile =
        "ssb-workload"
        </> printf "query-%s-%s-%03d.plan.txt" lbl (maybe "unlim" show mem) windex
  liftIO $ createDirectoryIfMissing True "ssb-workload"
  recordPlan planFile ref $ reverse transitions
  liftIO $ putStrLn $ "Plan file: " ++ planFile
  storeCpp lbl windex cppCode
  return transitions

type Seconds = Double
startTime :: IORef Seconds
{-# NOINLINE startTime #-}
startTime = unsafePerformIO $ getSecsI >>= newIORef

getSecsI :: IO Seconds
getSecsI = fromRational . toRational . utctDayTime <$> getCurrentTime

getSecs :: IO Seconds
getSecs = do
  t0 <- readIORef startTime
  t <- getSecsI
  return $ t - t0

storeCpp :: WLabel -> WIndex -> CppCode -> SSBGlobalSolveM ()
storeCpp lbl i cpp = do
  mem <- gets $ budget . globalGCConfig
  lift2 $ do
    createDirectoryIfMissing True "ssb-workload"
    let path =
          "ssb-workload"
          </> printf "query-%s-%s-%03i.cpp" lbl (maybe "unlim" show mem) i
    writeFile path cpp
    secs <- getSecs
    putStrLn $ printf "[%f sec]Wrote C++ code for query %d in: %s" secs i path

softFail :: AShow a => a -> IO ()
softFail a = putStrLn $ ashow a

ssbRunGlobalSolve :: SSBGlobalSolveM a -> IO ()
ssbRunGlobalSolve m = do
  ssbGlobalConf <- getSsbGlobalConf
  runGlobalSolve ssbGlobalConf (softFail . ashow) $ void m

#ifdef EXAMPLE
singleQuery :: WLabel -> IO ()
singleQuery lbl =
  void
  $ ssbRunGlobalSolve
  $ runQuery lbl 0 Verbose
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

getMats :: SSBGlobalSolveM [(NodeRef N,PageNum)]
getMats = globalizePlanT $ do
  ns <- nodesInState [Initial Mat,Concrete NoMat Mat,Concrete Mat Mat]
  mapM (\n -> (n,) <$> totalNodePages n) ns
reportMats msg = do
  mats <- getMats
  lift2 $ putStrLn msg
  lift2 $ putStrLn $ "mat nodes: " ++ ashow mats
  pgs <- globalizePlanT getDataSize
  lift2 $ putStrLn $ "Pages used: " ++ show pgs


data Verbosity = Verbose | Quiet
type QueryId = Int
type WIndex = Int
actualMain :: WLabel -> Verbosity -> [(WIndex,QueryId)] -> IO ()
actualMain lbl verbosity qs = ssbRunGlobalSolve $ forM_ qs $ \(wi,qi) -> do
  reportMats $ "Pre query: " ++ show (wi,qi)
  case IM.lookup qi ssbQueriesMap of
    Nothing -> throwAStr $ printf "No such query %d" qi
    Just query -> do
      transitions <- runQuery lbl verbosity wi query
      liftIO $ putStrLn $ "Transitions " ++ ashow (wi,qi)
      liftIO $ putStrLn $ ashow transitions
      reportMats $ "Post query: " ++ show (wi,qi)

type ImagePath = FilePath
type QueryPath = FilePath
type GraphPath = FilePath
type IntermediatesPath = FilePath

renderGraph
  :: SSBQuery
  -> SSBGlobalSolveM (IntermediatesPath,QueryPath,ImagePath,GraphPath)
renderGraph query = do
  gr <- gets $ propNet . globalGCConfig
  interms0 <- gets
    $ fmap3 (runIdentity . qnfOrigDEBUG')
    . refAssocs
    . nrefToQnfs
    . globalClusterConfig
  interms <- globalizePlanT $ forM interms0 $ \(ref,q) -> do
    pgs <- totalNodePages ref
    return (ref,pgs,Sym . unLatex . toLatex <$> q)
  mats <- getMats
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
    writeFile dotPath $ simpleRender (fst <$> mats) gr
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

workload :: [(WIndex, QueryId)]
workload = take 30 $ zip [1 ..] [1 .. ]

workload1 :: [(WIndex, QueryId)]
workload1 = zip [1 ..] [1 .. 13]

ssbMainWorkload :: IO ()
ssbMainWorkload = do
  let secs = 60
  putStrLn "Building normal workload..."
  timeout (secs * 1000000) (actualMain "main" Verbose workload) >>= \case
    Nothing -> putStrLn $ printf "TIMEOUT after %ds" secs
    Just () -> putStrLn "Done!"

recordPlan
  :: FilePath
  -> NodeRef N
  -> [Transition T N]
  -> SSBGlobalSolveM ()
recordPlan path slv trns = do
  mats <- fmap2 fst getMats
  Latex ltx <- dropReader (gets getGlobalConf) $ latexPlan slv mats trns
  lift2 $ writeFile path ltx

ssbMainIndiv :: IO ()
ssbMainIndiv = do
  let secs = 60
  putStrLn "Building individuals..."
  timeout (secs * 1000000) (forM_ workload1 $ \w ->
                            actualMain "indiv" Verbose [w]) >>= \case
    Nothing -> putStrLn $ printf "TIMEOUT after %ds" secs
    Just () -> putStrLn "Done!"
