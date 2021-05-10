{-# LANGUAGE RecordWildCards #-}
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

module FluiDB.Classes
  (RunningContext
  ,MonadFakeIO(..)
  ,existing
  ,inDir
  ,realIOOps
  ,IOOps(..)) where

import Data.Utils.Debug
import Data.Utils.Functors
import Data.Utils.Function
import Data.Utils.Hashable
import Data.Utils.AShow
import Data.CppAst as CC
import Data.String
import Control.Monad.Writer
import Control.Monad.Reader
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

data IOOps m =
  IOOps
  { ioLiftIO :: IO () -> m ()
   ,ioReadFile :: FilePath -> m String
   ,ioWriteFile :: FilePath -> String -> m ()
   ,ioFileExists :: FilePath -> m Bool
   ,ioCmd :: forall e s t n .
        String
        -> [String]
        -> GlobalSolveT e s t n m (OutStr,ErrStr)
   ,ioDie :: forall a . String -> m a
   ,ioSetCurrentDir :: FilePath -> m ()
   ,ioGetCurrentDir :: m FilePath
   ,ioLogMsg :: String -> m ()
  }

class Monad m => MonadFakeIO m where
  ioOps :: IOOps m
  default ioOps :: IOOps m
  ioOps = unsafeFakeIOOps

existing :: forall e s t n m .
           (Monad m, MonadFakeIO m) =>
           FilePath
         -> GlobalSolveT e s t n m FilePath
existing fp = do
  ex <- ioFileExists ioOps fp
  if ex
    then return fp else lift2 $ ioDie ioOps $ printf "Can't find file: %s" fp

inDir :: (Monad m, MonadFakeIO m) => FilePath -> m a -> m a
inDir dir mon = do
  rollback <- ioGetCurrentDir ioOps
  ioSetCurrentDir ioOps dir
  ret <- mon
  ioSetCurrentDir ioOps rollback
  return ret

instance MonadFakeIO Identity

unsafeFakeIOOps :: Monad m => IOOps m
unsafeFakeIOOps =
  IOOps
  { ioFileExists = const $ return False
   ,ioWriteFile = traceM ... printf "Writing %s <- '%s'"
   ,ioReadFile = error . printf "We don't support reading: ioReadFile \"%s\""
   ,ioLogMsg = traceM
   ,ioCmd = \cxx args
      -> lift2 $ error $ printf "can't run command $ %s %s" cxx $ unwords args
   ,ioSetCurrentDir = const $ return ()
   ,ioGetCurrentDir = error $ printf "can't get directory"
   ,ioLiftIO = undefined
   ,ioDie = error
  }
realCmd
  :: (MonadError (GlobalError e s t n) m,MonadIO m)
  => [Char]
  -> [String]
  -> m (String,String)
realCmd cxx args = do
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
realIOOps :: IOOps IO
realIOOps =
  IOOps
  { ioFileExists = doesFileExist
   ,ioWriteFile = writeFile
   ,ioReadFile = readFile
   ,ioCmd = realCmd
   ,ioSetCurrentDir = setCurrentDirectory
   ,ioGetCurrentDir = getCurrentDirectory
   ,ioLiftIO = id
   ,ioLogMsg = putStrLn
   ,ioDie = die
  }

instance MonadFakeIO IO where

instance (MonadFakeIO m) => MonadFakeIO (StateT s m)
instance (MonadFakeIO m) => MonadFakeIO (ReaderT r m)
instance (MonadFakeIO m,Monoid w) => MonadFakeIO (WriterT w m)
instance (MonadFakeIO m,IsString e)
  => MonadFakeIO (ExceptT e m) where
  ioOps = unsafeFakeIOOps --  { ioDie = throwError . fromString }
