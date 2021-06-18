{-# OPTIONS_GHC -Wno-unused-matches #-}
module Data.Codegen.Run (runCpp,tmpDir,runProc,mkProc) where

import           Control.Monad
import           GHC.IO.Exception
import           System.Directory
import           System.Environment
import           System.FilePath
import           System.IO
import           System.IO.Temp
import           System.Process
import           Text.Printf

mkProc :: FilePath -> [String] -> CreateProcess
mkProc = proc

-- Starts operating in a temporary directory. The first tmpDir call
-- creates a directory in /tmp and nested one create directories
-- inside. The state of nesting is managed by FLUIDB_IN_TMP_DIR.
type DirPath = FilePath
tmpDir :: String -> (DirPath -> IO a) -> IO a
tmpDir opId m = do
  (rootTmp,unsetDir) <- lookupEnv "FLUIDB_IN_TMP_DIR" >>= \case
    Nothing
     -> (,unsetEnv "FLUIDB_IN_TMP_DIR") <$> getCanonicalTemporaryDirectory
    Just curDir -> return (curDir,setEnv "FLUIDB_IN_TMP_DIR" curDir)
  dir <- createTempDirectory rootTmp opId
  setEnv "FLUIDB_IN_TMP_DIR" dir
  ret <- withCurrentDirectory dir $ m dir
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
runProc prc = tmpDir "runProc" $  \dirp -> do
  let cmdFile = dirp </> ".cmd"
  writeFile cmdFile $ show $ cmdspec prc
  (_in,Just outHndl,Just errHndl,procHndl)
    <- createProcess prc { std_err = CreatePipe,std_out = CreatePipe }
  waitForProcess procHndl >>= \case
    ExitFailure exitCode -> do
      let errFile = dirp </> "exec.err"
      let outFile = dirp </> "exec.out"
      transferToFile errHndl errFile
      transferToFile outHndl outFile
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
  let exeFile = dirp </> "exec.out"
  -- Compile the C++ file
  runProc $ mkProc "c++" [cppFile,"-o",exeFile]
  -- Run the executable
  runProc $ mkProc exeFile []

transferToFile :: Handle -> FilePath -> IO ()
transferToFile rhndl fname =
  withFile fname WriteMode $ \whndl -> hEachLine rhndl $ hPutStrLn whndl

hEachLine :: Handle -> (String -> IO ()) -> IO ()
hEachLine h f = whileIO (hIsEOF h) $ hGetLine h >>= f
