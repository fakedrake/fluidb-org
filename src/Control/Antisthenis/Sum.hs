{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Control.Antisthenis.Sum (SumTag) where

import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Types
import           Control.Monad.Identity
import           Control.Utils.Free
import           Data.Coerce
import           Data.Proxy
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Nat
import           Data.Utils.Tup
import           GHC.Generics

data SumTag p
type SumExtParams p = ExtParams (SumTag p) p

-- | Addition does not have an absorbing element. Therefore all
-- elements need to be evaluated to get a rigid result. However it is
-- often the case in antisthenis that a computation does not stop at a
-- final result but when a lower bound is met. The ideal case would be
-- to have a bound derivative and to move in the most promising side
-- but we do not so we will follow the greedy approach of finishing
-- with each element in sequence.
instance (Ord (ZBnd (SumTag p)),SumExtParams p) => BndRParams (SumTag p) where
  type ZErr (SumTag p) = ExtError p
  type ZBnd (SumTag p) = Min (MechVal p)
  type ZRes (SumTag p) = Sum (MechVal p)
  bndLt Proxy = (<)
  exceedsCap _ = extExceedsCap (Proxy :: Proxy (p,SumTag p))


data SumPartialRes p
  = SumPartErr (ZErr (SumTag p))
  | SumPart (Sum (MechVal p))
  | SumPartInit
  deriving Generic
instance Default (SumPartialRes p) where def = SumPartInit

sprMap
  :: (Sum (MechVal p) -> Sum (MechVal p)) -> SumPartialRes p -> SumPartialRes p
sprMap f = \case
  SumPart a -> SumPart $ f a
  a         -> a
instance (AShow (MechVal p),AShow (ZErr (SumTag p)))
  => AShow (SumPartialRes p)

instance (AShow2 (MechVal p) (ExtCap p)
         ,Ord (MechVal p) -- for ZBnd
         ,Subtr (MechVal p)
         ,Semigroup (MechVal p)
         ,Zero (MechVal p)
          -- For updating the cap
         ,HasLens (ExtCap p) (Min (MechVal p))
         ,Zero (ExtCap p)
         ,SumExtParams p
         ,AShow2 (ExtError p) (ExtEpoch p)
         ,AShowV (ExtCoEpoch p)) => ZipperParams (SumTag p) where
  type ZEpoch (SumTag p) = ExtEpoch p
  type ZCoEpoch (SumTag p) = ExtCoEpoch p
  type ZCap (SumTag p) = ExtCap p
  type ZPartialRes (SumTag p) = SumPartialRes p
  type ZItAssoc (SumTag p) =
    SimpleAssoc [] (ZBnd (SumTag p))
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = sumEvolutionControl
     ,evolutionStrategy = sumEvolutionStrategy
     ,evolutionEmptyErr = error "No arguments provided to the sum."
    }
  putRes newBnd (partialRes,newZipper) =
    (\() -> add partialRes newBnd) <$> newZipper
    where
      add SumPartInit bnd = toPartial bnd
      add e@(SumPartErr _) _ = e
      add (SumPart partRes) bnd = case bnd of
        BndBnd
          b -> SumPart $ coerce b <> coerce partRes -- will be registered as it.
        BndRes SumInf -> SumPart partRes
        BndRes r -> SumPart $ r <> partRes
        BndErr e -> SumPartErr e
      toPartial = \case
        BndBnd v      -> SumPart $ coerce v
        BndRes SumInf -> SumPartInit
        BndRes s      -> SumPart $ coerce s
        BndErr e      -> SumPartErr e
  replaceRes oldBnd newBnd (oldRes,newZipper) = case negMin oldBnd of
    Nothing -> Nothing
    Just oldBnd' -> Just
      $ putRes newBnd (sprMap (`sub` oldBnd') oldRes,newZipper)
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ conf { confCap = newCap }
    where
      newCap = case zRes z of
        SumPartErr _ -> CapVal zero -- zero cap
        SumPartInit -> confCap conf
        SumPart partRes -> case confCap conf of
          -- partRes is the total sum so far. We offset the global cap
          -- by the sum so far so that the local cap ensures that the
          -- cursor process, when ran, does not cause the overall
          -- bound to exceed the global cap. This may generate weird
          -- caps like negative values. That is ok as it should be
          -- handled by the evolutionControl function. Note again that
          -- zRes does not include the value under cursor.
          CapVal cap  -> CapVal $ subCap cap partRes
          ForceResult -> ForceResult

subCap :: forall cap v . (Subtr v,HasLens cap (Min v)) => cap -> Sum v -> cap
subCap cap s = modL defLens (coerce . go . coerce :: Min v -> Min v) cap where
  go :: Sum v -> Sum v
  go s' = maybe (error "subtracting infinity from cap") (sub s') $ negSum s

-- | Return the result expected or Nothing if there is more evolving
-- that needs to happen before a valid (ie that satisfies the cap)
-- result can be produced. This can only return bounds.
sumEvolutionControl
  :: forall p m .
  (Semigroup (MechVal p)
  -- ,HasLens (ZCap (SumTag p)) (Min (MechVal p))
  ,Zero (ZCap (SumTag p))
  ,Zero (ZBnd (SumTag p))
  ,Ord (MechVal p)
  ,AShow2 (ExtError p) (MechVal p)
  ,SumExtParams p, AShow (ExtCap p))
  => GConf (SumTag p)
  -> Zipper (SumTag p) (ArrProc (SumTag p) m)
  -> Maybe (BndR (SumTag p))
sumEvolutionControl conf z = fmap traceRes $ case zFullResSum z of
  SumPartErr e -> Just $ BndErr e
  SumPartInit -> case confCap conf of
    -- If the local bound is negative then
    CapVal cap -> if isNegative cap then Just $ BndBnd zero else trace "sum1" Nothing
    _          -> trace "sum2" Nothing
  SumPart bnd -> case confCap conf of
    CapVal cap ->
      if exceedsCap @(SumTag p) Proxy cap (coerce bnd :: ZBnd (SumTag p))
      then Just $ BndBnd $ coerce bnd else trace "sum3" Nothing
    ForceResult -> trace "sum4" Nothing
  where
    -- traceRes = id
    traceRes r = trace ("return(sum): " ++ ashow (zId z,r)) r

-- | The full result includes both zRes and zCursor.
zFullResSum
  :: (AShow2 (ExtError p) (MechVal p),Semigroup (MechVal p))
  => Zipper (SumTag p) p0
  -> SumPartialRes p
zFullResSum z = sprMap (maybe id ((<>) . coerce) cursorM) $ zRes z
  where
    cursorM = fst3 $ runIdentity $ zCursor z

sumEvolutionStrategy
  :: (Zero (MechVal p),Monad m)
  => FreeT (Cmds (SumTag p)) m x
  -> m
    (Maybe (ResetCmd (FreeT (Cmds (SumTag p)) m x))
    ,Either (ZCoEpoch (SumTag p),BndR (SumTag p)) x)
sumEvolutionStrategy = recur Nothing
  where
    recur rst (FreeT m) = m >>= \case
      Pure a -> return (rst,Right a)
      Free cmds0 -> go cmds0
        where
          go cmds = case cmdItCoit cmds of
            CmdItInit it _ini ->
              recur' $ it $ (\((a,b),c) -> (a,b,c)) . simpleAssocPopNEL
            CmdIt it ->
              recur' $ it $ (\((a,b),c) -> (a,b,c)) . simpleAssocPopNEL
            CmdInit ini -> recur' ini
            CmdFinished (ExZipper z) ->
              return (rst,Left (getConst $ zCursor z,zToRes z))
            where
              recur' = recur $ Just $ cmdReset cmds
              zToRes z = case zRes z of
                SumPart bnd  -> BndRes $ coerce bnd
                SumPartInit  -> BndRes zero
                SumPartErr e -> BndErr e
