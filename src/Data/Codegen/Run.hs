{-# LANGUAGE CPP #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}
module Data.Codegen.Run (runCpp,tmpDir,runProc,mkProc) where

import           Control.Exception
import           Control.Monad
import           Data.Utils.Debug
import           Data.Utils.Functors
import           GHC.IO.Exception
import           System.Directory
import           System.Environment
import           System.FilePath
import           System.IO
import           System.IO.Temp
import           System.Process

mkProc :: FilePath -> [String] -> CreateProcess
mkProc prog args = (proc prog args){cwd=Nothing}

-- Starts operating in a temporary directory. The first tmpDir call
-- creates a directory in /tmp and nested one create directories
-- inside. The state of nesting is managed by FLUIDB_IN_TMP_DIR.
type DirPath = FilePath
tmpDir :: String -> (DirPath -> IO a) -> IO a
tmpDir opId m = do
  canonTmp <- (</> "fluidb_temps") <$> getCanonicalTemporaryDirectory
  (rootTmp,unsetDir :: IO ()) <- lookupEnv "FLUIDB_IN_TMP_DIR" <&> \case
    Nothing     -> (canonTmp,unsetEnv "FLUIDB_IN_TMP_DIR")
    Just curDir -> (curDir,setEnv "FLUIDB_IN_TMP_DIR" curDir)
  createDirectoryIfMissing True canonTmp
  dir <- createTempDirectory rootTmp opId
  setEnv "FLUIDB_IN_TMP_DIR" dir
  ret <- withCurrentDirectory dir
    $ m dir `onException` unsetEnv "FLUIDB_IN_TMP_DIR"
  unsetDir
  -- We don't want this to be removed if there is an error so that we
  -- can inspect the situation.
  removeDirectoryRecursive dir
  return ret

whileIO :: IO Bool -> IO () -> IO ()
whileIO bM body = go
  where
    go = do
      b <- bM
      when b $ body >> go

-- | Runs process.
runProc :: CreateProcess -> IO ()
runProc prc = do
  dirp <- maybe getCurrentDirectory return (cwd prc)
  let cmdFile = dirp </> "exec.cmd"
  writeFile cmdFile $ show $ cmdspec prc
  (_in,Just outHndl,Just errHndl,procHndl)
    <- createProcess prc { std_err = CreatePipe,std_out = CreatePipe }
  waitForProcess procHndl >>= \case
    ExitFailure exitCode -> do
      let errFile = dirp </> "exec.err"
      let outFile = dirp </> "exec.out"
      transferToFile "stderr" errHndl errFile
      transferToFile "stdout" outHndl outFile
      fail
        $ printf
          "Failed command(%d): %s.\n\tstderr: %s,\n\tstdout: %s\n\tcmd: %s\n"
          exitCode
          (show $ cmdspec prc)
          errFile
          outFile
          cmdFile
    ExitSuccess -> return ()

-- | Runs C++ code represented as a string. It uses a tempdir to do
-- all the work.
runCpp :: String -> IO ()
runCpp str = tmpDir "runCpp" $ \dirp -> do
  let cppFile = dirp </> "exec.cpp"
  let exeFile = dirp </> "exec.exe"
  writeFile cppFile str
  -- Compile the C++ file
  traceM $ "Compiling: c++ " ++ cppFile ++ " -o " ++ exeFile
  tmpDir "compilation" $ \dir -> runProc $ mkProc "c++" ["-std=c++17",cppFile,"-o",exeFile]
  -- Run the executable
  traceM "Running..."
  tmpDir "run_compiled" $ \dir -> runProc $ mkProc exeFile []

transferToFile :: String -> Handle -> FilePath -> IO ()
transferToFile title rhndl fname = withFile fname WriteMode $ \whndl -> do
  putStrLn $ "Lines found in " ++ title ++ ":"
  hEachLine rhndl $ \l -> do
    putStrLn l
    hPutStrLn whndl l

hEachLine :: Handle -> (String -> IO ()) -> IO ()
hEachLine h f = whileIO (not <$> hIsEOF h) $ hGetLine h >>= f

#ifdef TEST_CODE
type CppCode = String
type Header = String
type Symbol = String
sysInclude :: Header -> CppCode
sysInclude = printf "#include <%s>\n"

declOStream :: Symbol -> FilePath -> CppCode
declOStream sym = printf "std::fstream %s;%s.open(\"%s\",std::ios::out); \n" sym sym

putLine :: Symbol -> String -> CppCode
putLine = printf "%s << \"%s\" << std::endl;\n"

mainFn :: Int -> CppCode -> CppCode
mainFn exit body = printf "int main () {\n%s return %d;\n}" body exit

runCommands :: Int -> IO ()
runCommands exit =
  tmpDir "command1" $ \dir -> do
  runCpp
    $ sysInclude "iostream" ++ sysInclude "fstream"
    ++ mainFn exit (declOStream "f" (dir </> "file") ++ putLine "f" "hello!")
#endif
