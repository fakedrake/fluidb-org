{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Control.Antisthenis.Convert
  (Conv(..)
  ,GenericConv(..)
  ,convMinSum
  ,convSumMin
  ,convAndOr
  ,convOrAnd
  ,coerceConv
  ,convArrProc) where

import           Control.Antisthenis.Bool
import           Control.Antisthenis.Minimum
import           Control.Antisthenis.Sum
import           Control.Antisthenis.Types
import qualified Control.Category            as C
import           Data.Coerce
import           Data.Profunctor
import           Data.Utils.Nat

data Conv w w' =
  Conv
  { convEpoch :: ZEpoch w' -> ZEpoch w
   ,convCap   :: ZCap w' -> ZCap w
   ,convRes   :: ZRes w -> ZRes w'
   ,convBnd   :: ZBnd w -> ZBnd w'
   ,convErr   :: ZErr w -> ZErr w'
  }

instance C.Category Conv where
  id =
    Conv { convEpoch = id,convCap = id,convRes = id,convBnd = id,convErr = id }
  a . b =
    Conv
    { convEpoch = convEpoch b . convEpoch a
     ,convCap = convCap b . convCap a
     ,convRes = convRes a . convRes b
     ,convBnd = convBnd a . convBnd b
     ,convErr = convErr a . convErr b
    }

type GEq a b = Coercible a b

-- |Generic conversion between tags describing computation.
data GenericConv w w' where
  GenericConv
    :: (GEq (ZRes w) (ZRes w')
       ,GEq (ZBnd w) (ZBnd w')
       ,GEq (ZErr w) (ZErr w')
       ,GEq (ZEpoch w) (ZEpoch w')
       ,GEq (ZCap w) (ZCap w'))
    => GenericConv w w'

coerceConv :: GenericConv w w' -> Conv w w'
coerceConv GenericConv =
  Conv
  { convEpoch = coerce
   ,convCap = coerce
   ,convRes = coerce
   ,convBnd = coerce
   ,convErr = coerce
  }

convArrProc
  :: (ZCoEpoch w' ~ ZCoEpoch w,Monad m)
  => Conv w w'
  -> ArrProc w m
  -> ArrProc w' m
convArrProc conv = dimap (convConf conv) (convBndR conv)

convConf :: Conv w w' -> Conf w' -> Conf w
convConf Conv {..} Conf {..} =
  Conf { confEpoch = convEpoch confEpoch
        ,confCap = convCap <$> confCap
        ,confTrPref = confTrPref
       }

convBndR :: Conv w w' -> BndR w -> BndR w'
convBndR Conv{..} = \case
  BndRes r -> BndRes $ convRes r
  BndBnd b -> BndBnd $ convBnd b
  BndErr e -> BndErr $ convErr e

convAndOr :: Conv (BoolTag And m) (BoolTag Or m)
convAndOr = coerceConv GenericConv
convOrAnd :: Conv (BoolTag Or m) (BoolTag And m)
convOrAnd = coerceConv GenericConv


-- We only need mempty from this monoid
convSumMin :: Conv (SumTag p) (MinTag p)
convSumMin =
  Conv
  { convEpoch = id
   ,convCap = coerce
   ,convRes = coerce
   ,convBnd = coerce
   ,convErr = coerce
  }

convMinSum :: Conv (MinTag p) (SumTag p)
convMinSum =
  Conv
  { convEpoch = id
   ,convCap = coerce
   ,convRes = coerce
   ,convBnd = coerce
   ,convErr = coerce
  }
