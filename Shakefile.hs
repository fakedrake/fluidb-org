{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators #-}
import Control.Monad.Reader
import Debug.Trace
import Development.Shake
import Development.Shake.Rule
import Development.Shake.Command
import Development.Shake.FilePath
import Development.Shake.Util
import Control.Monad.Except
import Data.List.Extra
import Text.Printf
import Control.Exception.Extra

data Config =
  Config
  { cnfStackRoot :: Maybe FilePath
   ,cnfStackArgs :: [String]
   ,cnfNeedFiles :: Action ()
  }
defConf :: Config
defConf = Config Nothing [] (return ())
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
    Just a -> return a
  where
    getVal ["local-install-root",v] = Just v
    getVal _ = Nothing

haskellExec :: String -> FDBAction FilePath
haskellExec exeName = do
  root <- localInstallRoot
  return $ root </> "bin" </> exeName

type Cmd = [String]
data StackCmd = StackRun | StackBuild | StackPath
stackCmd :: Config -> StackCmd -> [String] -> Cmd
stackCmd conf scmd args = "stack" : workDir ++ rest
  where
    workDir = maybe [] (\x -> ["--work-dir",x]) $ cnfStackRoot conf
    rest = case scmd of
      StackBuild -> "build" : cnfStackArgs conf ++ args
      StackPath -> "path" : args
      StackRun -> "run" : cnfStackArgs conf ++ args

execRule :: Config -> String -> Rules FilePath
execRule conf execName = do
  let execPath = "_build/bin" </> execName
  execPath %> \out -> do
    putInfo $ printf "Building executable %s (%s)" execName execPath
    needSrcFiles
    intermPath <- runReaderT (haskellExec execName) conf
    cmd_ (RemEnv "STACK_IN_NIX_SHELL")
      $ stackCmd conf StackBuild ["fluidb:exe:" ++ execName]
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
readdumpConf = defConf { cnfNeedFiles = need ["tools/ReadDump/Main.hs"] }
branchesConf :: Config
branchesConf =
  Config
  { cnfStackRoot = Just branchesDir
   ,cnfStackArgs = ["--ghc-options","-DVERBOSE_SOLVING"]
   ,cnfNeedFiles = needSrcFiles
  }
  where
    branchesDir = ".branches-stack-dir/"

pathsFileRule :: Config -> Rules ()
pathsFileRule conf = do
  let paths = runReader pathsFileR conf
  paths %> \out -> noNixCmd (FileStdout out) $ stackCmd conf StackPath []

main :: IO ()
main =
  shakeArgs shakeOptions { shakeVerbosity = Verbose,shakeFiles = "_build" } $ do
    want [tokenBranch]
    benchmarkExec <- execRule branchesConf "benchmark"
    readdumpExec <- execRule readdumpConf "readdump"
    pathsFileRule branchesConf
    pathsFileRule readdumpConf
    logDump %> \out -> do
      putInfo $ "Making the log dump: " ++ out
      need [benchmarkExec]
      noNixCmd (Timeout 10) (EchoStderr False) (FileStderr out) benchmarkExec
    tokenBranch %> \tok -> do
      putInfo $ "Building the branch files. The token required is " ++ tok
      need [logDump,readdumpExec]
      cmd_ ["mkdir","-p",branchesRoot]
      noNixCmd (Timeout 10) readdumpExec
