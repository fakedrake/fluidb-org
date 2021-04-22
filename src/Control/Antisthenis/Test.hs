{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE DeriveGeneric          #-}
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
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Control.Antisthenis.Test (withTrail,incrTill,zeroAfter,FixStateT(..)) where

import Control.Antisthenis.Lens
import Data.Utils.Debug
import Control.Arrow
import Data.Void
import Control.Antisthenis.ATL.Transformers.Mealy
import Control.Antisthenis.Types
import Control.Antisthenis.Zipper
import Control.Monad.Cont
import Control.Monad.Reader
import Control.Monad.State


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
  -> Cap (ZCap w)
  -> ArrProc w m
incrTill name (step,asRes) (Cap cap) = mkMealy $ recur $ BndBnd 0
  where
    recur :: BndR w -> Conf w -> MB (Conf w) (BndR w) m Void
    recur cntr conf = do
      conf' <- trace ("[" ++ name ++ "] Yielding: " ++ ashow cntr)
        $ yieldMB cntr
      if confEpoch conf' > confEpoch conf then recurGuarded (BndBnd 0) conf'
        else recurGuarded (incr cntr) conf'
    recurGuarded res conf = case confCap conf of
      WasFinished -> error "We should have finished"
      Cap c -> error "The cap is static.."
      MinimumWork -> recur res conf
      ForceResult
       -> finishMB $ BndRes $ asRes $ finalRes
      DoNothing -> do
        conf' <- yieldMB res
        recurGuarded res conf'
    finalRes = head $ dropWhile (< cap) $ iterate step 0
    incr :: BndR w -> BndR w
    incr = \case
      BndBnd x -> let x' = step x
        in if x' < cap then BndBnd x' else BndRes $ asRes x'
      x -> x

zeroAfter
  :: forall w m .
  (Monad m,ZipperParams w,Num (ZBnd w),Num (ZRes w),AShowBndR w)
  => Int
  -> ArrProc w m
zeroAfter i = mkMealy $ const $ do
  forM_ [1 :: Int .. 4] $ \x -> do
    let bnd = BndBnd 0 :: BndR w
    traceM $ "[Zero] Yielding(" ++ show x ++ "): " ++ ashow bnd
    yieldMB bnd
  let res = BndRes 0 :: BndR w
  traceM $ "[Zero] Final Yielding: " ++ ashow res
  finishMB res

var :: MonadState s m
    => s :>: (MealyArrow (Kleisli m) a b)
    -> MealyArrow (Kleisli m) a b
var lens = MealyArrow $ Kleisli $ \a -> do
  MealyArrow (Kleisli c) <- gets $ getL lens
  (nxt,r) <- c a
  modify $ modL lens $ const nxt
  return (nxt,r)

newtype FixStateT f m a =
  FixStateT { runFixStateT :: f (FixStateT f m) -> m (a,f (FixStateT f m)) }
  deriving (Functor,Applicative,Monad,MonadState (f (FixStateT f m)),MonadFail
           ,MonadReader r) via StateT (f (FixStateT f m)) m
instance MonadTrans (FixStateT f) where
  lift m = FixStateT $ \a -> (,a) <$> m
  {-# INLINE lift #-}


-- | Handle the error.
handleBndErr
  :: Monad m
  => (forall a . (Maybe (ZErr w) -> m a) -> m a)
  -> ArrProc w m
  -> ArrProc w m
handleBndErr handle = recur
  where
    recur
      (MealyArrow (Kleisli c)) = MealyArrow $ Kleisli $ \conf -> handle $ \case
      Just e -> return (MealyArrow $ Kleisli c,BndErr e)
      Nothing -> first recur <$> c conf



withTrail
  :: MonadReader trail m
  => (trail -> Either (ZErr w) trail)
  -> ArrProc w m
  -> ArrProc w m
withTrail insUniq = handleBndErr $ \handle -> asks insUniq >>= \case
  Left e -> handle $ Just e
  Right tr -> local (const tr) $ handle Nothing
