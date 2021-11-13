{-# LANGUAGE CPP                    #-}
{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE DerivingVia            #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE MultiWayIf             #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Control.Antisthenis.Test (withTrail,incrTill,zeroAfter) where

import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Types
import           Control.Arrow
import           Control.Monad.Reader
import           Control.Monad.Writer
import           Data.Utils.Debug
import           Data.Utils.Functors
import           Data.Void


incrTill
  :: forall w m .
  (Monad m
  ,ZipperParams w
  ,Ord (ZEpoch w)
  ,Ord (ZBnd w)
  ,Num (ZBnd w)
  ,AShowBndR w
  ,ZBnd w ~ ZCap w)
  => String
  -> (ZBnd w
      -> ZCap w
     ,ZBnd w
      -> ZRes w)
  -> ZCap w
  -> ArrProc w m
incrTill name (step,asRes) cap = mkMealy $ recur $ BndBnd 0
  where
    recur
      :: BndR w -> Conf w -> MB (Conf w) (BndR w) (WriterT (ZCoEpoch w) m) Void
    recur cntr _ = do
      conf' <- trace ("[" ++ name ++ "] Yielding: " ++ ashow cntr)
        $ yieldMB cntr
      recurGuarded conf'
    recurGuarded conf = case confCap conf of
      CapVal _c   -> error "The cap is static.."
      ForceResult -> finishMB $ BndRes $ asRes finalRes
    finalRes = head $ dropWhile (< cap) $ iterate step 0

zeroAfter
  :: forall w m .
  (Monad m,ZipperParams w,Num (ZBnd w),Num (ZRes w),AShowBndR w)
  => Int
  -> ArrProc w m
zeroAfter i = mkMealy $ const $ do
  forM_ [1 :: Int .. i] $ \x -> do
    let bnd = BndBnd 0 :: BndR w
    traceM $ "[Zero] Yielding(" ++ show x ++ "): " ++ ashow bnd
    yieldMB bnd
  let res = BndRes 0 :: BndR w
  traceM $ "[Zero] Final Yielding: " ++ ashow res
  finishMB res

-- | Handle the error.
handleBndErr
  :: (Monoid (ZCoEpoch w),Monad m)
  => (forall a . (Maybe (ZErr w) -> m a) -> m a)
  -> ArrProc w m
  -> ArrProc w m
handleBndErr handle = recur
  where
    recur (ArrProc c) = ArrProc $ \conf -> handle $ \case
      Just e  -> return (mempty,(ArrProc c,BndErr e))
      Nothing -> fmap2 (first recur) $ c conf
    recur _ = error "unreachable"

withTrail
  :: (Monoid (ZCoEpoch w),MonadReader trail m)
  => (trail -> Either (ZErr w) trail)
  -> ArrProc w m
  -> ArrProc w m
withTrail insUniq = handleBndErr $ \handle -> asks insUniq >>= \case
  Left e   -> handle $ Just e
  Right tr -> local (const tr) $ handle Nothing
