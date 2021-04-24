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

module Database.FluiDB.Running.Classes
  ( RunningContext
  , MonadFakeIO(..)
  , existing
  , inDir
  ) where

import Data.String
import Control.Monad.Writer
import Control.Monad.Reader
import qualified Database.FluiDB.CppAst as CC
import Database.FluiDB.Debug
import           Control.Monad.Morph
import           Control.Monad.State
import           Database.FluiDB.AShow
import           Database.FluiDB.Running.Types
import           Control.Monad.Except
import           Control.Monad.Identity
import           Database.FluiDB.Utils
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
  ioLiftIO :: IO () -> m ()
  default ioLiftIO :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) => IO () -> m ()
  ioLiftIO = lift . ioLiftIO
  ioReadFile :: FilePath -> m String
  default ioReadFile :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                         FilePath -> m String
  ioReadFile = lift . ioReadFile
  ioWriteFile :: FilePath -> String -> m ()
  default ioWriteFile :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                          FilePath -> String -> m ()
  ioWriteFile x y = lift $ ioWriteFile x y
  ioFileExists :: FilePath -> m Bool
  default ioFileExists :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) => FilePath -> m Bool
  ioFileExists = lift . ioFileExists
  ioCmd :: String -> [String] -> GlobalSolveT e s t n m (OutStr,ErrStr)
  default cmd :: (MonadTrans tr,MonadFakeIO m0,m ~ tr m0) =>
                Monad m => String -> [String] -> GlobalSolveT e s t n m (OutStr,ErrStr)
  cmd cxx args = hoist (hoist lift) $ cmd cxx args
  ioDie :: String -> m a
  default ioDie :: (MonadTrans tr,MonadFakeIO m0,m ~ tr m0) => String -> m a
  ioDie = lift . ioDie
  ioSetCurrentDir :: FilePath -> m ()
  default ioSetCurrentDir :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                              FilePath -> m ()
  ioSetCurrentDir = lift . ioSetCurrentDir
  ioGetCurrentDir :: m FilePath
  default ioGetCurrentDir :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) =>
                              m FilePath
  ioGetCurrentDir = lift ioGetCurrentDir
  ioLogMsg :: String -> m ()
  default ioLogMsg :: (MonadTrans t,MonadFakeIO m0,m ~ t m0) => String -> m ()
  ioLogMsg = lift . ioLogMsg

existing :: forall e s t n m .
           (Monad m, MonadFakeIO m) =>
           FilePath
         -> GlobalSolveT e s t n m FilePath
existing fp = do
  ex <- ioFileExists fp
  if ex then return fp else lift2 $ ioDie $ printf "Can't find file: %s" fp

inDir :: (Monad m, MonadFakeIO m) => FilePath -> m a -> m a
inDir dir mon = do
  rollback <- ioGetCurrentDir
  ioSetCurrentDir dir
  ret <- mon
  ioSetCurrentDir rollback
  return ret

instance MonadFakeIO Identity where
  ioFileExists _ = return False
  ioWriteFile = ioLogMsg ... printf "Writing %s <- '%s'"
  ioReadFile = ioDie . printf "We don't support reading: ioReadFile \"%s\""
  ioLogMsg = traceM
  cmd cxx args =
    lift2 $ ioDie $ printf "can't run command $ %s %s" cxx $ unwords args
  ioSetCurrentDir _ = return ()
  ioGetCurrentDir = dieFake $ printf "can't get directory"
  ioLiftIO = undefined
  ioDie = error



instance MonadFakeIO IO where
  ioFileExists = doesFileExist
  ioWriteFile = writeFile
  ioReadFile = readFile
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
  ioSetCurrentDir = setCurrentDirectory
  ioGetCurrentDir = getCurrentDirectory
  ioLiftIO = id
  ioLogMsg = putStrLn
  ioDie = die

instance (MonadFakeIO m) => MonadFakeIO (StateT s m)
instance (MonadFakeIO m) => MonadFakeIO (ReaderT r m)
instance (MonadFakeIO m,Monoid w) => MonadFakeIO (WriterT w m)
instance (MonadFakeIO m,IsString e) => MonadFakeIO (ExceptT e m) where
  ioDie = throwError . fromString
