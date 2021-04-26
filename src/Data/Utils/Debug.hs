{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Data.Utils.Debug
  ( wrapTrace
  , wrapTraceShow
  , traceM
  , traceTM
  , trace
  , traceT
  , wrapTraceT
  , assertM
  , printf
  , ashow
  ) where

import           Data.Utils.AShow
import           Text.Printf
#if ! defined(QUIET_MODE)
import           Control.Exception
import           Data.Time.Clock
import           Debug.Trace
import           System.IO.Unsafe

wrapTrace :: Monad m => String -> m a -> m a
wrapTrace msg x = do
  traceM $ "[Before] " ++ msg
  ret <- x
  traceM $ "[After] " ++ msg
  return ret
wrapTraceShow :: (Show a, Monad m) => String -> m a -> m a
wrapTraceShow msg x = do
  traceM $ "[Before] " ++ msg
  ret <- x
  traceM $ "[After (ret: " ++ show ret ++ ")] " ++ msg
  return ret
assertM :: Monad m => m Bool -> m ()
assertM bM = do {b <- bM; assert b $ return ()}

initSeconds :: Double
initSeconds = unsafeDupablePerformIO getSeconds
{-# NOINLINE initSeconds #-}
getDSecs :: IO Double
getDSecs = (+ (-initSeconds)) <$> getSeconds

getSeconds :: IO Double
getSeconds = getCurrentTime >>= return . fromRational . toRational . utctDayTime


-- | The argument is a function that given the timestamp section
-- builds the string.
traceT :: String -> a -> a
traceT str expr = unsafeDupablePerformIO $ do
  t <- getDSecs
  traceIO $ printf "[%.03f] %s" t str
  return expr
traceTM :: Applicative m => String -> m ()
traceTM str = traceT str $ pure ()
wrapTraceT :: Monad m => String -> m a -> m a
wrapTraceT msg x = do
  traceTM $ "[Before] " ++ msg
  ret <- x
  traceTM $ "[After] " ++ msg
  return ret

#else
trace :: String -> a -> a
trace = const id
traceM :: Monad m => String -> m ()
traceM = const $ return ()
wrapTrace :: Monad m => String -> m a -> m a
wrapTrace = const id
wrapTraceShow :: (Show a, Monad m) => String -> m a -> m a
wrapTraceShow = const id
assertM :: Monad m => m Bool -> m ()
assertM = const $ return ()
traceT :: String -> a -> a
traceT _ a = a
traceTM :: Applicative m => String -> m ()
traceTM _ = pure ()
wrapTraceT :: Monad m => String -> m a -> m a
wrapTraceT = wrapTrace
#endif
