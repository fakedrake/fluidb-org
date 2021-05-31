import Development.Shake
import Development.Shake.Rule
import Development.Shake.Command
import Development.Shake.FilePath
import Development.Shake.Util
import Control.Monad.Except

data BuildConf = BuildConf {
  bcBuildArgs :: [String],
  bcExe :: String
  }

splitOn :: Eq a => a -> [a] -> Maybe ([a], [a])
splitOn v = go [] where
  go prev (x:xs) = if x == v then Just (reverse prev, xs) else go (x:prev) xs
  go prev [] = Nothing

branchesBuildConf :: BuildConf
branchesBuildConf =
  BuildConf
  { bcBuildArgs = ["--ghc-options=-DVERBOSE_SOLVING"]
   ,bcExe = "benchmark"
  }

sb :: BuildConf -> Action ()
sb cnf = do
  cmd_ $ "stack build -j " ++ unwords (bcBuildArgs cnf)

logDump = "/tmp/benchmark.out"
pathsFile = "_build/paths.txt"

localInstallRoot :: Action FilePath
localInstallRoot = do
  need [pathsFile]
  ls <- liftIO
    $ filter (maybe False ((== "local-install-root") . fst) . splitOn ':')
    . lines
    <$> readFile "_build/paths.txt"
  case ls of
    [] -> fail "Couldn't find 'local-install-root' in '_build/paths.txt'"
    [a] -> return a
    _:_ -> fail
      "More than one instances of 'local-install-root' in '_build/paths.txt'"

main :: IO ()
main = shakeArgs shakeOptions { shakeFiles = "_build" } $ do
  want [logDump]
  phony "branches" $ do
    sb branchesBuildConf
  pathsFile %> \out -> do
    Stdout contents <- cmd $ "stack path"
    liftIO $ writeFile out contents
  logDump %> \out -> do
    benchmarkExe <- (</> "bin/benchmark") <$> localInstallRoot
    need [benchmarkExe]
    cmd_ $ "stack run benchmark > " ++ out
