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
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.Nat
import           Data.Utils.Tup
import           GHC.Generics

data SumTag p v

-- | Addition does not have an absorbing element. Therefore all
-- elements need to be evaluated to get a rigid result. However it is
-- often the case in antisthenis that a computation does not stop at a
-- final result but when a lower bound is met. The ideal case would be
-- to have a bound derivative and to move in the most promising side
-- but we do not so we will follow the greedy approach of finishing
-- with each element in sequence.
instance ExtParams p => BndRParams (SumTag p v) where
  type ZErr (SumTag p v) = ExtError p
  type ZBnd (SumTag p v) = Min v
  type ZRes (SumTag p v) = Sum v

data SumPartialRes p v
  = SumPartErr (ZErr (SumTag p v))
  | SumPart (Sum v)
  | SumPartInit
  deriving Generic
instance Default (SumPartialRes p v) where def = SumPartInit

sprMap :: (Sum v -> Sum v) -> SumPartialRes p v -> SumPartialRes p v
sprMap f = \case
  SumPart a -> SumPart $ f a
  a         -> a
instance (AShow v,AShow (ZErr (SumTag p v)))
  => AShow (SumPartialRes p v)

instance (AShow v
         ,AShow (ExtCap p)
         ,Ord v
         ,Subtr v
         ,Semigroup v
         ,Zero v
         ,HasLens (ExtCap p) (Min v)
         ,Zero (ExtCap p)
         ,ExtParams p
         ,AShow (ExtError p)
         ,AShow (ExtEpoch p)
         ,AShow (ExtCoEpoch p)) => ZipperParams (SumTag p v) where
  type ZEpoch (SumTag p v) = ExtEpoch p
  type ZCoEpoch (SumTag p v) = ExtCoEpoch p
  type ZCap (SumTag p v) = ExtCap p
  type ZPartialRes (SumTag p v) = SumPartialRes p v
  type ZItAssoc (SumTag p v) =
    SimpleAssoc [] (ZBnd (SumTag p v))
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
    tr
    $ extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ conf { confCap = newCap }
    where
      tr = id
      -- tr =
      --   trace
      --     ("zLocalizeConf(sum) " ++ ashow (zId z,zRes z,confCap conf,newCap))
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
  :: forall v p m .
  (HasLens (ZCap (SumTag p v)) (Min v)
  ,Semigroup v
  ,Zero v
  ,Ord v
  ,AShow v
  ,AShow (ExtError p)
  ,ExtParams p)
  => GConf (SumTag p v)
  -> Zipper (SumTag p v) (ArrProc (SumTag p v) m)
  -> Maybe (BndR (SumTag p v))
sumEvolutionControl conf z = fmap traceRes $ case zFullResSum z of
  SumPartErr e -> Just $ BndErr e
  SumPartInit -> case confCap conf of
    -- If the local bound is negative then
    CapVal cap -> if negativeCap cap then Just $ BndBnd zero else Nothing
    _          -> Nothing
  SumPart bnd -> case confCap conf of
    CapVal cap ->
      ifLt cap (coerce bnd :: Min v) (Just $ BndBnd $ coerce bnd) Nothing
    ForceResult -> Nothing
  where
    negativeCap cap = getL defLens cap < (zero :: Min v)
    traceRes = id
    -- traceRes r = trace ("return(sum): " ++ ashow (zId z,r)) r

-- | The full result includes both zRes and zCursor.
zFullResSum :: Semigroup v => Zipper (SumTag p v) p0 -> SumPartialRes p v
zFullResSum z = sprMap (maybe id ((<>) . coerce) cursorM) $ zRes z where
  cursorM = fst3 $ runIdentity $ zCursor z

sumEvolutionStrategy
  :: (Zero v,Monad m)
  => (BndR (SumTag p v) -> x)
  -> FreeT
    (ItInit (ExZipper (SumTag p v)) (SimpleAssoc [] (ZBnd (SumTag p v))))
    m
    (x,BndR (SumTag p v))
  -> m (x,BndR (SumTag p v))
sumEvolutionStrategy fin = recur
  where
    recur (FreeT m) = m >>= \case
      Pure a -> return a
      Free f -> case f of
        CmdItInit _it ini -> recur ini
        CmdIt it -> recur $ it $ (\((a,b),c) -> (a,b,c)) . simpleAssocPopNEL
        CmdInit ini -> recur ini
        CmdFinished (ExZipper z)
          -> let res = case zRes z of
                   SumPart bnd  -> BndRes $ coerce bnd
                   SumPartInit  -> BndRes zero
                   SumPartErr e -> BndErr e in return (fin res,res)
