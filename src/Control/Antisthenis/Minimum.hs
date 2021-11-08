{-# LANGUAGE Arrows                 #-}
{-# LANGUAGE CPP                    #-}
{-# LANGUAGE DeriveFoldable         #-}
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
{-# LANGUAGE QuantifiedConstraints  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE StandaloneDeriving     #-}
{-# LANGUAGE TemplateHaskell        #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TypeApplications       #-}
module Control.Antisthenis.Minimum
  (MinTag) where

import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.Lens
import           Control.Antisthenis.Types
import           Control.Applicative                hiding (Const (..))
import           Control.Monad.Identity
import           Control.Utils.Free
import           Data.Foldable
import qualified Data.List.NonEmpty                 as NEL
import           Data.Maybe
import           Data.Proxy
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Debug
import           Data.Utils.Nat
import           Data.Utils.Tup
import           GHC.Generics

data MinTag p
type MinExtParams p = ExtParams (MinTag p) p

data ConcreteVal w
  = OnlyErrors [ZErr w]
  | FoundResult (ZRes w)
  deriving Generic
instance (AShowV [ZErr w],AShow (ZRes w))
  => AShow (ConcreteVal w)
emptyConcreteVal :: ConcreteVal w
emptyConcreteVal = OnlyErrors []

-- Ideally this should be a heap but for now this will not grow beyond
-- 10 elements so we are good.
data MinAssocList f w a =
  MinAssocList
  { malElements :: f (ZBnd w,a)
    -- | The concrete value has the following states:
    -- * we have encountered only errors
    -- * we have encountered at least one concrete result
    -- * We have encountered NO results
   ,malConcrete :: ConcreteVal w
  }
  deriving (Generic,Functor,Foldable)
instance (AShow (f (ZBnd w,a)),AShowBndR w,AShow a,AShowV [ZErr w])
  => AShow (MinAssocList f w a)

-- | The minimum bound.
data SecondaryBound w
  = SecRes (ZRes w) -- The secondary bound comes from Res
  | SecBnd (Maybe (ZRes w)) (ZBnd w) -- The secondary bound comes from a Bnd (the ZRes
                                      -- is just for debug)
  | NoSec -- First actual lower bound or all other res are errors.
  | NeverSec -- There won't be a secondary because there is only one
             -- process in the pipeline.
  deriving Generic
instance (AShow (ZRes w),AShow (ZBnd w))
  => AShow (SecondaryBound w)
zSecondaryBound
  :: forall f r x p .
  (Foldable f,Functor f,Ord (MechVal p),MinExtParams p)
  => Zipper' (MinTag p) f r x
  -> SecondaryBound (MinTag p)
zSecondaryBound z = case malConcrete mal of
  OnlyErrors _ -> noSecondaryBound
  FoundResult firstRes -> case elemBnd of
    Just bnd -> if bndLt @(MinTag p) Proxy firstRes bnd then SecRes firstRes
      else SecBnd (Just firstRes) bnd
    Nothing -> SecRes firstRes -- there are no other results.
  where
    -- How do we encode the lack of a secondary bound. If we are still
    -- going through the inits there is NoSec (ie. while there is no
    -- secondary bound ATM, we shouldn't get too carried away as there
    -- are inits that might be very small) but if can't find another
    -- secondary bound and there are no inits to go through we should
    -- just concede that there will never be a secondary bound and we
    -- should go all-in in the process under the cursor.
    noSecondaryBound = if null $ bgsInits $ zBgState z then NeverSec else NoSec
    mal = bgsIts $ zBgState z
    elemBnd = foldl' go Nothing $ fst <$> malElements mal
      where
        go :: Maybe (ZBnd (MinTag p))
           -> ZBnd (MinTag p)
           -> Maybe (ZBnd (MinTag p))
        go Nothing r   = Just r
        go (Just r0) r = Just $ minBnd @p Proxy r0 r

minBnd
  :: forall p .
  (Ord (MechVal p),MinExtParams p)
  => Proxy p
  -> ZBnd (MinTag p)
  -> ZBnd (MinTag p)
  -> ZBnd (MinTag p)
minBnd _ a b = if bndLt @(MinTag p) Proxy a b then a else b


zModConcrete
  :: (ZItAssoc w ~ MinAssocList [] w)
  => (ConcreteVal w -> ConcreteVal w)
  -> Zipper' w f p pr
  -> Zipper' w f p pr
zModConcrete f z = z { zBgState = zsModIts $ zBgState z }
  where
    zsModIts zst = zst { bgsIts = malModIts $ bgsIts zst } where
      malModIts m = m{malConcrete=f $ malConcrete m}

instance AssocContainer (MinAssocList [] w) where
  type KeyAC (MinAssocList [] w) = ZBnd w
  type NonEmptyAC (MinAssocList [] w) = MinAssocList NEL.NonEmpty w
  acInsert k a (MinAssocList m c) =
    MinAssocList ((k,a) NEL.:| m) c
  acUnlift (MinAssocList m a) =
    MinAssocList (NEL.toList m) a
  acEmpty = MinAssocList [] emptyConcreteVal
  acNonEmpty (MinAssocList mal x) =
    (`MinAssocList` x) <$> NEL.nonEmpty mal

type L1 = NEL.NonEmpty
barrel :: L1 a -> L1 (L1 a)
barrel x@(_ NEL.:| []) = pure x
barrel x@(a NEL.:| a':as) = x NEL.:| go [a] a' as
  where
    go left root [] = pure $ root NEL.:| left
    go left root (root':right) =
      (root NEL.:| root' : left ++ right)
      : go (root : left) root' right


minimumOn :: Ord b => L1 (b,a) -> a
minimumOn ((b,a) NEL.:| as) =
  snd
  $ foldl' (\(b0,a0) (b1,a1) -> if b0 < b1 then (b0,a0) else (b1,a1)) (b,a) as

-- | Return the minimum of the list.
popMinAssocList
  :: (Ord (ZBnd w),AShowV (ZBnd w))
  => MinAssocList L1 w a
  -> (ZBnd w,a,MinAssocList [] w a)
popMinAssocList (MinAssocList m c) = check (v,a,res)
  where
    check x =
      if any (\(bnd,_a) -> v > bnd) $ malElements res
      then error "Min fail" else x
    res = MinAssocList vs c
    (v,a) NEL.:| vs = minimumOn $ (\a' -> (fst $ NEL.head a',a')) <$> barrel m

topMinAssocList
  :: Ord (ZBnd w) => MinAssocList L1 w a -> ZBnd w
topMinAssocList mal = elemBnd
  where
    elemBnd = let x NEL.:| xs = fst <$> malElements mal in foldl' min x xs

instance (MinExtParams p,Ord (MechVal p)) => BndRParams (MinTag p) where
  type ZErr (MinTag p) = ExtError p
  type ZBnd (MinTag p) = Min (MechVal p)
  type ZRes (MinTag p) = Min (MechVal p)
  bndLt Proxy = (<)
  exceedsCap _ = extExceedsCap (Proxy :: Proxy (p,MinTag p))

instance (AShow (MechVal p)
         ,AShow (ExtCoEpoch p)
         ,Ord (MechVal p)
         ,AShow (ExtCap p)
         ,Zero (ExtCap p)
         ,Zero (MechVal p)
         ,MinExtParams p
         ,NoArgError (ExtError p)
         ,HasLens (ExtCap p) (Min (MechVal p))
         ,AShow (ExtError p)) => ZipperParams (MinTag p) where
  type ZCap (MinTag p) = ExtCap p
  type ZEpoch (MinTag p) = ExtEpoch p
  type ZCoEpoch (MinTag p) = ExtCoEpoch p
  type ZPartialRes (MinTag p) = ()
  type ZItAssoc (MinTag p) =
    MinAssocList [] (MinTag p)
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = minEvolutionControl
     ,evolutionStrategy = minEvolutionStrategy
     ,evolutionEmptyErr = noArgumentsError
    }
  -- We only need to do anything if the result is concrete. A
  -- non-error concrete result in combined with the result so far. In
  -- the case where the result is an error the error is
  -- updated. Bounded results are already stored in the associative
  -- structure.
  putRes newBnd ((),newZipper) = case newBnd of
    BndRes r -> zModConcrete (putResult r) newZipper
    BndErr e -> zModConcrete (putError e) newZipper
    BndBnd _ -> newZipper -- already taken care of implicitly
    where
      -- Only put error if there is no concrete solution.
      putError e = \case
        OnlyErrors es     -> OnlyErrors $ e : es
        r@(FoundResult _) -> r
      -- Drop any possible errors and replace them with the
      -- result. Otherwise combine the results.
      putResult r = \case
        OnlyErrors _   -> FoundResult r
        FoundResult r0 -> FoundResult $ min r r0
  -- Remove the bound from the result and insert the new one. Here we
  -- do not need to remove anything since the value of the bound is
  -- directly inferrable from the container of intermediates. Remember
  -- that we never really need to remove anything from the final
  -- result as the result is implicit in the container. `Nothing`
  -- means that we failed to remove oldBnd so this will always
  -- succeed.
  replaceRes _oldBnd newBnd (oldRes,newZipper) =
    Just $ putRes newBnd (oldRes,newZipper)
  -- | The secondary bound becomes the cap when it is smaller than the
  -- global cap.
  --
  -- Also handles the combination of epoch and coepoch that dictates
  -- whether we must reset.
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ case (zSecondaryBound z,confCap conf) of
      (NeverSec,_) -> conf
      (NoSec,_) -> conf { confCap = CapVal zero }
      (SecRes res,CapVal c) -> conf { confCap = CapVal $ chooseCapRes c res }
      (SecBnd _ bnd,CapVal c) -> conf { confCap = CapVal $ chooseCapBnd c bnd }
      (SecRes res,ForceResult) -> conf { confCap = CapVal $ rgetL res }
      (SecBnd _ bnd,ForceResult) -> conf { confCap = CapVal $ rgetL bnd }
    where
      chooseCapBnd :: ZCap (MinTag p) -> ZBnd (MinTag p) -> ZCap (MinTag p)
      chooseCapBnd cap res =
        if exceedsCap @(MinTag p) Proxy cap res then cap else let r = rgetL res
          in trace ("Update cap: " ++ ashow r) r
      chooseCapRes :: ZCap (MinTag p) -> ZRes (MinTag p) -> ZCap (MinTag p)
      chooseCapRes cap res =
        if exceedsCap @(MinTag p) Proxy cap res then cap else rgetL res

-- | Given a proposed solution and the configuration from which it
-- came return a bound that matches the configuration or Nothing if
-- the zipper should keep evolving to get to a proper resilt.
minEvolutionControl
  :: forall p r x .
  (AShow (MechVal p)
  ,AShow (ExtCap p)
  ,AShow (ExtError p)
  ,Ord (MechVal p)
  ,MinExtParams p)
  => GConf (MinTag p)
  -> Zipper' (MinTag p) Identity r x
  -> Maybe (BndR (MinTag p))
minEvolutionControl conf z = case confCap conf of
  ForceResult -> res >>= \case
    BndBnd bnd -> case zSecondaryBound z of
      SecRes r        -> if r < bnd then Just $ BndRes r else Nothing
      SecBnd _ secBnd -> if secBnd >= bnd then Nothing else error "oops"
      _               -> Nothing
    x -> return x
  CapVal cap -> res >>= \case
    BndBnd bnd -> case zSecondaryBound z of
      SecRes r -> if bndLt @(MinTag p) Proxy r bnd then Just $ BndRes r
        else if exceedsCap @(MinTag p) Proxy cap bnd
          then Just $ BndBnd bnd else Nothing
      _sec -> if exceedsCap @(MinTag p) Proxy cap bnd
        then Just $ BndBnd bnd else Nothing
    x -> Just x
  where
    -- THE result so far. The cursor is assumed to always be either an
    -- init or the most promising.
    res = case fst3 (runIdentity $ zCursor z) of
      Just r -> Just $ BndBnd r
      Nothing -> case zSecondaryBound z of
        NeverSec   -> Nothing
        NoSec      -> Nothing
        SecRes r   -> Just $ BndRes r
        SecBnd _ b -> Just $ BndBnd b

minBndR
  :: forall p .
  (Ord (MechVal p),MinExtParams p)
  => BndR (MinTag p)
  -> BndR (MinTag p)
  -> BndR (MinTag p)
minBndR = curry $ \case
  (_,e@(BndErr _))    -> e
  (e@(BndErr _),_)    -> e
  (BndBnd a,BndBnd b) -> BndBnd $ minBnd @p Proxy a b
  (BndRes a,BndRes b) -> BndRes $ minBnd @p Proxy a b
  (BndRes a,BndBnd b) -> if a < b then BndRes a else BndBnd b
  (BndBnd a,BndRes b) -> if a < b then BndBnd a else BndRes b

zIsFinished :: Zipper' (MinTag p) f r x -> Bool
zIsFinished z =
  null (bgsInits $ zBgState z)
  && isNothing (acNonEmpty $ bgsIts $ zBgState z)

-- | Turn the zipper into a value.
--
-- XXX: if the cap is DoNothing we shoudl return the minimum
-- bound. even if Nothing is encountered.
zFullResultMin
  :: (Foldable f,AShow (MechVal p),Ord (MechVal p),MinExtParams p)
  => Zipper' (MinTag p) f r x
  -> Maybe (BndR (MinTag p))
zFullResultMin z = (curs `minBndR'` softBound `minBndR'` hardBound) <|> Nothing
  where
    minBndR' Nothing Nothing   = Nothing
    minBndR' x Nothing         = x
    minBndR' Nothing y         = y
    minBndR' (Just x) (Just y) = Just $ minBndR x y
    curs = BndBnd <$> (>>= fst3) (listToMaybe $ toList $ zCursor z)
    softBound = BndBnd . topMinAssocList <$> acNonEmpty (bgsIts $ zBgState z)
    hardBound = case malConcrete $ bgsIts $ zBgState z of
      OnlyErrors []    -> Nothing
      OnlyErrors (e:_) -> if zIsFinished z then Just $ BndErr e else Nothing
      FoundResult r    -> Just $ BndRes r

minEvolutionStrategy
  :: (Ord (MechVal p),Monad m,AShow (MechVal p),MinExtParams p)
  => FreeT (Cmds (MinTag p)) m x
  -> m
    (Maybe (ResetCmd (FreeT (Cmds (MinTag p)) m x))
    ,Either (ZCoEpoch (MinTag p),BndR (MinTag p)) x)
minEvolutionStrategy = recur Nothing
  where
    recur rst (FreeT m) = m >>= \case
      Pure a -> return (rst,Right a)
      Free cmd -> case cmdItCoit cmd of
        CmdItInit _it ini -> recur (Just $ cmdReset cmd) ini
        CmdIt it -> recur (Just $ cmdReset cmd) $ it popMinAssocList
        CmdInit ini -> recur (Just $ cmdReset cmd) ini
        CmdFinished (ExZipper x) -> return
          (rst
          ,Left (getConst $ zCursor x,fromMaybe undefined $ zFullResultMin x))
