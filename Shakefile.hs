{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators    #-}
import           Control.Exception.Extra
import           Control.Monad.Except
import           Control.Monad.Reader
import           Data.List.Extra
import           Data.Maybe
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

noNixCmd :: (Partial, CmdArguments args) => args :-> Action r
noNixCmd = cmd (RemEnv "STACK_IN_NIX_SHELL")


type Cmd = [String]
data StackCmd = StackRun | StackBuild | StackPath
stackCmd :: Config -> StackCmd -> [String] -> Cmd
stackCmd conf scmd args = "stack" : workDir ++ rest
  where
    workDir = maybe [] (\x -> ["--work-dir",x]) $ cnfStackRoot conf
    rest = case scmd of
      StackBuild -> ["build","-j","4"] ++ saCompile (cnfStackArgs conf) ++ args
      StackPath -> "path" : args
      StackRun ->
        saRun (cnfStackArgs conf) ++ ["exec",cnfExecName conf] ++ args


execTokRule :: Config -> String -> Rules FilePath
execTokRule conf execName = do
  let execTok =
        fromMaybe "." (cnfStackRoot conf)
        </> "_tokens"
        </> (execName <.> "token")
  phony execName $ need [execTok]
  execTok %> \out -> do
    putInfo $ printf "Building token %s (%s)" (cnfExecName conf) execTok
    needSrcFiles
    let command = stackCmd conf StackBuild ["fluidb:exe:" ++ cnfExecName conf]
    putInfo $ "Command: " ++ show command
    cmd_ (RemEnv "STACK_IN_NIX_SHELL") command
    cmd_ ["touch",out]
  return $ execTok

-- CONFIG
branchesRoot :: FilePath
branchesRoot = "/tmp/benchmark.out.bench_branches"
tokenBranch :: FilePath
tokenBranch = branchesRoot </> "branches000/branch0000.txt"

readdumpConf :: Config
readdumpConf =
  (defConf "readdump") { cnfNeedFiles = need ["tools/ReadDump/Main.hs"] }
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
   ,cnfStackArgs = mempty
   ,cnfNeedFiles = needSrcFiles
   ,cnfExecName = "benchmark"
  }

benchProfileConf :: Config
benchProfileConf =
  Config
  { cnfStackRoot = Just ".benchmark-profule-stack-dir/"
   ,cnfStackArgs = anyArgs ["--profile"]
   ,cnfNeedFiles = needSrcFiles
   ,cnfExecName = "benchmark"
  }

profileBenchmark :: FilePath
profileBenchmark = "/tmp/benchmark.prof"

main :: IO ()
main =
  shakeArgs shakeOptions { shakeVerbosity = Verbose,shakeFiles = "_build" } $ do
    -- want [tokenBranch]
    benchmarProfileExecTok <- execTokRule benchProfileConf "benchmark-profile"
    benchmarkExecTok <- execTokRule benchConf "benchmark-profile"
    benchmarkBranchesExecTok <- execTokRule branchesConf "benchmark-branches"
    readdumpExecTok <- execTokRule readdumpConf "readdump"
    logDump %> \out -> do
      need [benchmarkBranchesExecTok]
      putInfo $ "Running: " ++ out
      noNixCmd (Timeout 60) (EchoStderr False) (FileStderr out)
        $ stackCmd branchesConf StackRun []
    tokenBranch %> \tok -> do
      putInfo $ "Building the branch files. The token required is " ++ tok
      need [logDump,readdumpExecTok]
      cmd_ ["mkdir","-p",branchesRoot]
      noNixCmd $ stackCmd readdumpConf StackRun []
    phony "run-branches" $ need [tokenBranch]
    phony "run-benchmark-profile" $ do
      need [benchmarkProfileExecTok]
      noNixCmd $ stackCmd benchProfileConf StackRun ["+RTS","-p"]
    phony "run-benchmark" $ do
      need [benchmarkExecTok]
      noNixCmd $ stackCmd benchConf
    phony "clean" $ do
      cmd_ ["rm","-rf",benchmarkExecTok,benchmarkExecTok,readdumpExecTok,branchesRoot,logDump]
