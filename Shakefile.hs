import Development.Shake
import Development.Shake.Rule
import Development.Shake.Command
import Development.Shake.FilePath
import Development.Shake.Util
-- Shake extensions

root :: String -> (FilePath -> Bool) -> (FilePath -> Action ()) -> Rules ()
root help test act = addUserRule $ FileRule help $ \x -> if not
  $ test x then Nothing else Just $ ModeDirect $ do
  liftIO $ createDirectoryRecursive $ takeDirectory x
  act x
-- /Shake extension

data BuildConf = BuildConf {
  bcBuildArgs :: [String],
  bcExe :: String
  }

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

localInstallRoot :: Action FilePath
localInstallRoot = do
    need [ "_build/paths.txt" ]
    filter _ . lines <$> readFile "_build/paths.txt"


main :: IO ()
main = shakeArgs shakeOptions{shakeFiles="_build"} $ do

  phony "branches" $ do
    sb branchesBuildConf
  logDump %> \path -> do
    need [ benchmarkExe ]
    cmd_ $ "stack run benchmark > " ++ path
