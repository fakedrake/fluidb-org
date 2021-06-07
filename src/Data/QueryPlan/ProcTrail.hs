{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Data.QueryPlan.ProcTrail (withTrail,modTrail,mkEpoch) where

import Control.Monad.Writer
import Data.Maybe
import Control.Antisthenis.ATL.Transformers.Mealy
import Control.Monad.State
import Data.QueryPlan.Types
import Control.Antisthenis.ATL.Common
import Control.Antisthenis.ATL.Class.Functorial
import Data.NodeContainers
import Control.Antisthenis.Types
import Control.Arrow hiding ((>>>))

modTrailE
  :: Monoid (ZCoEpoch w)
  => (NTrail n -> Either (ZErr w) (NTrail n))
  -> Arr (NodeProc t n w) a (Either (ZErr w) a)
modTrailE f = mealyLift $ fromKleisli $ \a -> gets (f . npsTrail) >>= \case
  Left e -> return $ Left e
  Right r -> do
    modify $ \nps -> nps { npsTrail = r }
    return (Right a)

modTrail :: Monoid (ZCoEpoch w)
         => (NTrail n -> NTrail n)
         -> Arr (NodeProc t n w) a a
modTrail f = mealyLift $ fromKleisli $ \a -> do
  modify $ \nps -> nps { npsTrail = f $ npsTrail nps }
  return a
withTrail
  :: Monoid (ZCoEpoch w)
  => (NTrail n -> ZErr w)
  -> NodeRef n
  -> NodeProc t n w
  -> NodeProc t n w
withTrail cycleErr ref m =
  modTrailE putRef >>> (arr BndErr ||| m) >>> modTrail (nsDelete ref)
  where
    putRef ns =
      if ref `nsMember` ns then Left $ cycleErr ns else Right $ nsInsert ref ns
-- | Transfer the value of the epoch to the coepoch
mkEpoch
  :: (ZCoEpoch w ~ RefMap n Bool
     ,ZEpoch w ~ RefMap n Bool
     ,Monoid (ExtCoEpoch (PlanParams n)))
  => whenMat
  -> NodeRef n
  -> Arr (NodeProc t n w) (Conf w) (Either whenMat (Conf w))
mkEpoch whenMat ref = mealyLift $ fromKleisli $ \conf -> do
  let isMater = fromMaybe False $ ref `refLU` confEpoch conf
  tell $ refFromAssocs [(ref,isMater)]
  return $ if isMater then Left whenMat else Right conf
