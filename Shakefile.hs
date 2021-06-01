import Development.Shake
import Development.Shake.Rule
import Development.Shake.Command
import Development.Shake.FilePath
import Development.Shake.Util
import Control.Monad.Except
import Data.List.Extra
import Text.Printf

haskellFiles :: Action [FilePath]
haskellFiles = do
  Stdout files <- cmd "find src -name '*.hs'"
  return $ lines files

srcDeps :: Action ()
srcDeps = do
  fs <- haskellFiles
  need $ "Shakefile.hs":"package.yaml":fs

logDump = "/tmp/benchmark.out"
pathsFile = "_build/paths.txt"

localInstallRoot :: Action FilePath
localInstallRoot = do
  need [pathsFile]
  v <- liftIO
    $ firstJust (getVal . splitOn ": ") . lines <$> readFile pathsFile
  case v of
    Nothing -> fail "oops"
    Just a -> return a
  where
    getVal ["local-install-root",v] = Just v
    getVal _ = Nothing

haskellExec :: String -> Action FilePath
haskellExec exeName = do
  root <- localInstallRoot
  return $ root </> "bin" </> exeName

execRule :: [String] -> String -> Rules FilePath
execRule args execName  = do
  let execPath = "_build/bin" </> execName
  execPath %> \out -> do
    srcDeps
    intermPath <- haskellExec execName
    cmd_ $ "stack build fluidb:exe:" ++ execName ++ " " ++ unwords args
    cmd_ (printf "ln -s %s %s" intermPath out :: String)
  return $ execPath

main :: IO ()
main = shakeArgs shakeOptions { shakeFiles = "_build" } $ do
  want [logDump]
  benchmarkExec <- execRule ["--ghc-options=-DVERBOSE_SOLVING"] "benchmark"
  readdumpExec <- execRule [] "readdump"
  pathsFile %> \out -> cmd (FileStdout out) "stack path"
  logDump %> \out -> do
    need [benchmarkExec]
    cmd (Timeout 10) (EchoStderr False) (FileStderr out) "stack run benchmark"
