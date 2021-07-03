{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE TypeFamilies     #-}
module Data.QueryPlan.ProcTrail (withTrail,modTrail,mkEpoch) where

import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Common
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.Types
import           Control.Arrow                              hiding ((>>>))
import           Control.Monad.State
import           Control.Monad.Writer
import           Data.Maybe
import           Data.NodeContainers
import           Data.QueryPlan.Types
import           Data.Utils.AShow

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
    putRef ns
      | nsSize ns > 10 = error $ "Very long trail: " ++ ashow ns
      | ref `nsMember` ns = Left $ cycleErr ns
      | otherwise = Right $ nsInsert ref ns

-- | Transfer the value of the epoch to the coepoch. The epoch of a
-- node wrt to calculating costs is fully defined by the
-- materialization status of the node. The arrow produced by this
-- function operates on the WriterArrow (ZCoEpoch w) prooperty of
-- NodeProc. The materialization status which is the Epoch is updates
-- the co-epoch. The reader is reminded that antisthenis automatically
-- manages how the current epoch combined with the previous co-epoch
-- decide the continued legitimacy of the current value.
mkEpoch
  :: (ZCoEpoch w ~ RefMap n Bool
     ,ZEpoch w ~ RefMap n Bool
     ,Monoid (ExtCoEpoch (PlanParams n)))
  => (Conf w -> whenMat)
  -> NodeRef n
  -> Arr (NodeProc t n w) (Conf w) (Either whenMat (Conf w))
mkEpoch whenMat ref = mealyLift $ fromKleisli $ \conf -> do
  let isMater = fromMaybe False $ ref `refLU` confEpoch conf
  tell $ refFromAssocs [(ref,isMater)]
  return $ if isMater then Left $ whenMat conf else Right conf
