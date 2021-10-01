{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators    #-}
import           Control.Exception.Extra
import           Control.Monad.Except
import           Control.Monad.Reader
import           Data.List.Extra
import           Debug.Trace
import           Development.Shake
import           Development.Shake.Command
import           Development.Shake.FilePath
import           Development.Shake.Rule
import           Development.Shake.Util
import           Text.Printf

data Config =
  Config
  { cnfStackRoot :: Maybe FilePath
   ,cnfStackArgs :: StackArgs
   ,cnfNeedFiles :: Action ()
   ,cnfExecName  :: String
  }
data StackArgs = StackArgs { saCompile :: [String],saRun :: [String] }
emptyArgs :: StackArgs
emptyArgs = StackArgs [] []
compileArgs :: [String] -> StackArgs
compileArgs as = emptyArgs { saCompile = as }
runArgs :: [String] -> StackArgs
runArgs as = emptyArgs { saCompile = as }
anyArgs :: [String] -> StackArgs
anyArgs as = StackArgs as as
defConf :: String -> Config
defConf = Config Nothing emptyArgs (return ())
type FDBAction = ReaderT Config Action

needSrcFiles :: Action ()
needSrcFiles = do
  Stdout files <- cmd Shell "find src/ -name '*.hs'"
  let fs = lines files
  need $ "Shakefile.hs" : "package.yaml" : fs

logDump = "/tmp/benchmark.out"
pathsFileR :: MonadReader Config m => m FilePath
pathsFileR =
  asks
  $ maybe "_shake_build/paths.txt" (</> "_shake_build/paths.txt")
  . cnfStackRoot

pathsFileA :: FDBAction FilePath
pathsFileA = do
  ret <- pathsFileR
  lift $ need [ret]
  return ret

noNixCmd :: (Partial, CmdArguments args) => args :-> Action r
noNixCmd = cmd (RemEnv "STACK_IN_NIX_SHELL")

localInstallRoot :: FDBAction FilePath
localInstallRoot = do
  paths <- pathsFileA
  v <- liftIO $ firstJust (getVal . splitOn ": ") . lines <$> readFile paths
  case v of
    Nothing -> fail "oops"
    Just a  -> return a
  where
    getVal ["local-install-root",v] = Just v
    getVal _                        = Nothing

haskellExec :: String -> FDBAction FilePath
haskellExec exeName = do
  root <- localInstallRoot
  return $ root </> "bin" </> exeName

type Cmd = [String]
data StackCmd = StackRun | StackBuild | StackPath
stackCmd :: Config -> StackCmd -> [String] -> Cmd
stackCmd conf scmd args = "stack" : workDir ++ rest
  where
    workDir = maybe [] (\x -> [ "--work-dir", x ]) $ cnfStackRoot conf

    rest = case scmd of
        StackBuild -> [ "build" ] ++ saCompile (cnfStackArgs conf) ++ args
        StackPath -> "path" : args
        StackRun -> saRun (cnfStackArgs conf)
            ++ [ "exec", cnfExecName conf ] ++ args


execRule :: Config -> String -> Rules FilePath
execRule conf execName = do
  let execPath = "_build/bin" </> execName
  phony execName $ need [execPath]
  execPath %> \out -> do
    putInfo $ printf "Building executable %s (%s)" (cnfExecName conf) execPath
    needSrcFiles
    intermPath <- runReaderT (haskellExec $ cnfExecName conf) conf
    let command = stackCmd conf StackBuild ["fluidb:exe:" ++ cnfExecName conf]
    putInfo $ "Command: " ++ show command
    cmd_ (RemEnv "STACK_IN_NIX_SHELL") command
    exists <- doesFileExist out
    if exists then cmd_ ["touch",out] else cmd_
      (printf "ln -s %s %s" intermPath out
         :: String)
  return $ execPath

-- CONFIG
branchesRoot :: FilePath
branchesRoot = "/tmp/benchmark.out.bench_branches"
tokenBranch :: FilePath
tokenBranch = branchesRoot </> "branches000/branch0000.txt"

readdumpConf :: Config
readdumpConf =
  Config
  { cnfStackRoot = Nothing
   ,cnfStackArgs = emptyArgs
   ,cnfNeedFiles = need ["tools/ReadDump/Main.hs"]
   ,cnfExecName = "readdump"
  }

branchesConf :: Config
branchesConf =
  Config
  { cnfStackRoot = Just ".branches-stack-dir/"
   ,cnfStackArgs = compileArgs ["--ghc-options","-DVERBOSE_SOLVING"]
   ,cnfNeedFiles = needSrcFiles
   ,cnfExecName = "benchmark"
  }

benchConf :: Config
benchConf =
  Config
  { cnfStackRoot = Just ".benchmark-stack-dir/"
   ,cnfStackArgs = anyArgs ["--profile"]
   ,cnfNeedFiles = needSrcFiles
   ,cnfExecName = "benchmark"
  }

pathsFileRule :: Config -> Rules ()
pathsFileRule conf = do
  let paths = runReader pathsFileR conf
  paths %> \out -> do
    () <- noNixCmd (FileStdout out) $ stackCmd conf StackPath []
    putInfo $ "Listed haskell files in: " ++ out

profileBenchmark :: FilePath
profileBenchmark = "/tmp/benchmark.prof"

main :: IO ()
main =
  shakeArgs shakeOptions { shakeVerbosity = Verbose,shakeFiles = "_build" } $ do
    -- want [tokenBranch]
    benchmarkExec <- execRule benchConf "benchmark"
    benchmarkBranchesExec <- execRule branchesConf "benchmark-branches"
    readdumpExec <- execRule readdumpConf "readdump"
    pathsFileRule benchConf
    pathsFileRule branchesConf
    pathsFileRule readdumpConf
    logDump %> \out -> do
      need [benchmarkBranchesExec]
      putInfo $ "Running: " ++ out
      noNixCmd (Timeout 60) (EchoStderr False) (FileStderr out)
        $ stackCmd branchesConf StackRun []
      -- noNixCmd (Timeout 60) (EchoStderr False) (FileStderr out) benchmarkBranchesExec
    tokenBranch %> \tok -> do
      putInfo $ "Building the branch files. The token required is " ++ tok
      need [logDump,readdumpExec]
      cmd_ ["mkdir","-p",branchesRoot]
      noNixCmd $ stackCmd readdumpConf StackRun []
    phony "run-branches" $ do
      need [tokenBranch]
    phony "run-benchmark" $ do
      need [benchmarkExec]
      noNixCmd $ stackCmd benchConf StackRun ["+RTS","-p"]
