{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

module FluiDB.Classes (RunningContext,MonadFakeIO(..),existing,inDir) where

import Data.Utils.Debug
import Data.Utils.Functors
import Data.Utils.Function
import Data.Utils.Hashable
import Data.Utils.AShow
import Data.CppAst as CC
import Data.String
import Control.Monad.Writer
import Control.Monad.Reader
import           Control.Monad.Morph
import           Control.Monad.State
import           FluiDB.Types
import           Control.Monad.Except
import           Control.Monad.Identity
import           System.Directory
import           System.Exit
import           System.IO
import           System.Process

type RunningContext e s t n m qio =
  (AShowV e,AShowV s,
   MonadFakeIO m,CC.ExpressionLike e,
   Show qio, Hashables2 e s)

type OutStr = String
type ErrStr = String
class Monad m => MonadFakeIO m where
  liftFakeIO :: IO () -> m ()
  default liftFakeIO :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) => IO () -> m ()
  liftFakeIO = lift . liftFakeIO
  readFileFake :: FilePath -> m String
  default readFileFake :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                         FilePath -> m String
  readFileFake = lift . readFileFake
  writeFileFake :: FilePath -> String -> m ()
  default writeFileFake :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                          FilePath -> String -> m ()
  writeFileFake x y = lift $ writeFileFake x y
  fileExistFake :: FilePath -> m Bool
  default fileExistFake :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) => FilePath -> m Bool
  fileExistFake = lift . fileExistFake
  cmd :: String -> [String] -> GlobalSolveT e s t n m (OutStr,ErrStr)
  default cmd :: (MonadTrans tr,MonadFakeIO m0,m ~ tr m0) =>
                Monad m => String -> [String] -> GlobalSolveT e s t n m (OutStr,ErrStr)
  cmd cxx args = hoist (hoist lift) $ cmd cxx args
  dieFake :: String -> m a
  default dieFake :: (MonadTrans tr,MonadFakeIO m0,m ~ tr m0) => String -> m a
  dieFake = lift . dieFake
  setCurrentDirFake :: FilePath -> m ()
  default setCurrentDirFake :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                              FilePath -> m ()
  setCurrentDirFake = lift . setCurrentDirFake
  getCurrentDirFake :: m FilePath
  default getCurrentDirFake :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                              m FilePath
  getCurrentDirFake = lift getCurrentDirFake
  logMsg :: String -> m ()
  default logMsg :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) => String -> m ()
  logMsg = lift . logMsg

existing :: forall e s t n m .
           (Monad m, MonadFakeIO m) =>
           FilePath
         -> GlobalSolveT e s t n m FilePath
existing fp = do
  ex <- fileExistFake fp
  if ex then return fp else lift2 $ dieFake $ printf "Can't find file: %s" fp

inDir :: (Monad m, MonadFakeIO m) => FilePath -> m a -> m a
inDir dir mon = do
  rollback <- getCurrentDirFake
  setCurrentDirFake dir
  ret <- mon
  setCurrentDirFake rollback
  return ret

instance MonadFakeIO Identity where
  fileExistFake _ = return False
  writeFileFake = logMsg ... printf "Writing %s <- '%s'"
  readFileFake = dieFake . printf "We don't support reading: readFileFake \"%s\""
  logMsg = traceM
  cmd cxx args =
    lift2 $ dieFake $ printf "can't run command $ %s %s" cxx $ unwords args
  setCurrentDirFake _ = return ()
  getCurrentDirFake = dieFake $ printf "can't get directory"
  liftFakeIO = undefined
  dieFake = error



instance MonadFakeIO IO where
  fileExistFake = doesFileExist
  writeFileFake = writeFile
  readFileFake = readFile
  cmd cxx args = do
    prErr $ printf "$ %s %s" cxx $ unwords args
    (code,out,err) <- liftIO $ readProcessWithExitCode cxx args ""
    case code of
      ExitFailure c -> prErr (printf "Command failed (code: %d)" c)
        >> throwError (CommandError cxx args c)
      ExitSuccess -> do
        prErr "Command success!"
        return (out,err)
    where
      prErr = liftIO . hPutStrLn stderr
  setCurrentDirFake = setCurrentDirectory
  getCurrentDirFake = getCurrentDirectory
  liftFakeIO = id
  logMsg = putStrLn
  dieFake = die

instance (MonadFakeIO m) => MonadFakeIO (StateT s m)
instance (MonadFakeIO m) => MonadFakeIO (ReaderT r m)
instance (MonadFakeIO m,Monoid w) => MonadFakeIO (WriterT w m)
instance (MonadFakeIO m,IsString e) => MonadFakeIO (ExceptT e m) where
  dieFake = throwError . fromString
