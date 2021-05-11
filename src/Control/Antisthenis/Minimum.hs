{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Arrows #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveFunctor #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Control.Antisthenis.Minimum
  (minTest,MinTag) where

import Control.Monad.Writer
import Data.Proxy
import Data.Utils.FixState
import Control.Monad.Trans.Free
import GHC.Generics
import Data.Utils.Debug
import Data.Utils.Functors
import Data.Utils.Monoid
import Control.Antisthenis.Test
import Data.Utils.Tup
import Data.Utils.AShow
import Data.Foldable
import qualified Data.List.NonEmpty as NEL
import qualified Data.IntSet as IS
import Control.Antisthenis.VarMap
import Control.Monad.Reader
import Data.Maybe
import Data.Utils.Default
import Control.Antisthenis.AssocContainer
import Control.Antisthenis.Types
import Control.Antisthenis.Zipper

data MinTag p v

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
  { malElements :: (f (ZBnd w,a))
    -- | The concrete value has the following states:
    -- * we have encountered only errors
    -- * we have encountered at least one concrete result
    -- * We have encountered NO results
   ,malConcrete :: ConcreteVal w
  }
  deriving (Generic,Functor)
instance (AShow (f (ZBnd w,a)),AShowBndR w,AShow a,AShowV [ZErr w])
  => AShow (MinAssocList f w a)


-- | The minimum bound
malMinBound :: (Ord (ZBnd w),Foldable f) => MinAssocList f w a -> Maybe (ZBnd w,a)
malMinBound = foldl' findMin Nothing . malElements
  where
    findMin Nothing x = Just x
    findMin (Just a'@(v',_)) (a@(v,_)) = Just $ if v < v' then a else a'

zModConcrete
  :: (ZItAssoc w ~ MinAssocList [] w)
  => (ConcreteVal w -> ConcreteVal w)
  -> Zipper' w f p pr
  -> Zipper' w f p pr
zModConcrete f z = z { zBgState = zsModIts $ zBgState z }
  where
    zsModIts zst = zst { bgsIts = malModIts $ bgsIts zst } where
      malModIts m = m{malConcrete=f $ malConcrete m}

instance Foldable f => Foldable (MinAssocList f w) where
  foldr f v mal = foldr2 f v $ malElements mal

instance AssocContainer (MinAssocList [] w) where
  type KeyAC (MinAssocList [] w) = ZBnd w
  type NonEmptyAC (MinAssocList [] w) = MinAssocList NEL.NonEmpty w
  acInsert k a (MinAssocList m c) =
    MinAssocList ((k,a) NEL.:| m) c
  acUnlift (MinAssocList m a) =
    MinAssocList (NEL.toList m) a
  acEmpty = MinAssocList [] emptyConcreteVal
  acNonEmpty (MinAssocList mal x) =
    (\mal' -> MinAssocList mal' x) <$> NEL.nonEmpty mal


barrel :: NEL.NonEmpty a -> NEL.NonEmpty (NEL.NonEmpty a)
barrel x@(a0 NEL.:| as0) = x NEL.:| go [] a0 as0
  where
    go _pref _a [] = []
    go pref a (a':as) = (a' NEL.:| (a : pref)) : go (a : pref) a' as

minimumOn :: Ord b => NEL.NonEmpty (b,a) -> a
minimumOn ((b,a) NEL.:| as) =
  snd
  $ foldl' (\(b0,a0) (b1,a1) -> if b0 > b1 then (b1,a1) else (b0,a0)) (b,a) as

-- | Return the minimum of the list.
popMinAssocList
  :: Ord (ZBnd w)
  => MinAssocList NEL.NonEmpty w a
  -> (ZBnd w,a,MinAssocList [] w a)
popMinAssocList (MinAssocList m c) = (v,a,MinAssocList vs c)
  where
    (v,a) NEL.:| vs = minimumOn $ (\a' -> (fst $ NEL.head a',a')) <$> barrel m

topMinAssocList :: Ord (ZBnd w) => MinAssocList NEL.NonEmpty w a -> ZBnd w
topMinAssocList mal = elemBnd
  where
    elemBnd = let x NEL.:| xs = fst <$> malElements mal in foldl' min x xs

instance BndRParams (MinTag p a) where
  type ZErr (MinTag p a) = ExtError p
  type ZBnd (MinTag p a) = Min a
  type ZRes (MinTag p a) = Min a

instance (AShow a
         ,Eq a
         ,Ord a
         ,ExtParams p
         ,NoArgError (ExtError p)
         ,AShow (ExtError p)) => ZipperParams (MinTag p a) where
  type ZCap (MinTag p a) = Min a
  type ZEpoch (MinTag p a) = ExtEpoch p
  type ZCoEpoch (MinTag p a) = ExtCoEpoch p
  type ZCoEpoch (MinTag p a) = ExtCoEpoch p
  type ZPartialRes (MinTag p a) = ()
  type ZItAssoc (MinTag p a) =
    MinAssocList [] (MinTag p a)
  zprocEvolution =
    ZProcEvolution
    { evolutionControl =
        (\conf z -> deriveOrdSolution conf =<< zFullResultMin z)
     ,evolutionStrategy = minStrategy
     ,evolutionEmptyErr = noArgumentsError
    }
  -- We only need to do anything if the result is concrete. A
  -- non-error concrete result in combined with the result so far. In
  -- the case where the result is an error the error is
  -- updated. Bounded results are already stored in the associative
  -- structure.
  putRes newBnd ((),newZipper) = case newBnd of
    BndRes r -> trace ("putRes: " ++ ashow r)
      $ zModConcrete (putResult r) newZipper
    BndErr e -> trace ("putErr: " ++ ashow e)
      $ zModConcrete (putError e) newZipper
    BndBnd _ -> newZipper -- already taken care of implicitly
    where
      -- Only put error if there is no concrete solution.
      putError e = \case
        OnlyErrors es -> OnlyErrors $ e : es
        r@(FoundResult _) -> r
      -- Drop any possible errors and replace them with the
      -- result. Otherwise combine the results.
      putResult r = \case
        OnlyErrors _ -> FoundResult r
        FoundResult r0 -> FoundResult $ r <> r0
  -- Remove the bound from the result and insert the new one. Here we
  -- do not need to remove anything since the value of the bound is
  -- directly inferrable from the container of intermediates. Remember
  -- that we never really need to remove anything from the final
  -- result as the result is implicit in the container. `Nothing`
  -- means that we failed to remove oldBnd so this will always
  -- succeed.
  replaceRes _oldBnd newBnd (oldRes,newZipper) =
    Just $ putRes newBnd (oldRes,newZipper)
  -- The localized configuration asserts that the computation does not
  -- exceed the minimum bound established. If the minimum bound is
  -- concrete then we have finished the computatiuon. If there are
  -- still inits to be consumed do minimum work.
  --
  -- When the concrete result is an error, it is to be disregarded.
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ case (malConcrete $ bgsIts $ zBgState z) of
      FoundResult concreteBnd -> case malMinBound $ bgsIts $ zBgState z of
        Just (minBnd,_) -> conf { confCap = Cap $ concreteBnd <> minBnd }
        Nothing -> conf { confCap = Cap concreteBnd }
      OnlyErrors _ -> case malMinBound $ bgsIts $ zBgState z of
        Just (minBnd,_) -> conf { confCap = Cap minBnd }
        Nothing -> conf { confCap = MinimumWork }

-- | Given a proposed solution and the configuration from which it
-- came return a bound that matches the configuration or Nothing if
-- the zipper should keep evolving to get to a proper resilt.
deriveOrdSolution
  :: forall v p .
  (Ord v,AShow v,AShow (ExtError p))
  => Conf (MinTag p v)
  -> BndR (MinTag p v)
  -> Maybe (BndR (MinTag p v))
deriveOrdSolution conf res = case confCap conf of
  WasFinished -> Just $ trace ("reusing solution: " ++ ashowRes ashow' res) res
  DoNothing -> Just res
  MinimumWork -> Just res
  ForceResult -> case res of
    BndBnd _bnd -> Nothing
    x -> return x
  Cap cap -> case res of
    BndBnd bnd -> if bnd <= cap then Nothing else Just $ BndBnd bnd
    x -> return x

minBndR :: Ord v => BndR (MinTag p v) -> BndR (MinTag p v) -> BndR (MinTag p v)
minBndR = curry $ \case
  (_,e@(BndErr _)) -> e
  (e@(BndErr _),_) -> e
  (BndBnd a,BndBnd b) -> BndBnd $ min a b
  (BndRes a,BndRes b) -> BndRes $ min a b
  (BndRes a,BndBnd b) -> if a < b then BndRes a else BndBnd b
  (BndBnd a,BndRes b) -> if a < b then BndBnd a else BndRes b


zIsFinished :: Zipper' (MinTag p v) f r x -> Bool
zIsFinished z =
  null (bgsInits $ zBgState z)
  && isNothing (acNonEmpty $ bgsIts $ zBgState z)

-- | Turn the zipper into a value.
zFullResultMin
  :: (Foldable f,AShow v,Ord v)
  => Zipper' (MinTag p v) f r x
  -> Maybe (BndR (MinTag p v))
zFullResultMin z = case curs `minBndR'` softBound `minBndR'` hardBound of
  Nothing -> Nothing
  Just x -> Just x
  where
    minBndR' Nothing Nothing = Nothing
    minBndR' x Nothing = x
    minBndR' Nothing y = y
    minBndR' (Just x) (Just y) = Just $ minBndR x y
    curs = BndBnd <$> (>>= fst3) (listToMaybe $ toList $ zCursor z)
    softBound = BndBnd . topMinAssocList <$> acNonEmpty (bgsIts $ zBgState z)
    hardBound = case malConcrete $ bgsIts $ zBgState z of
      OnlyErrors [] -> Nothing
      OnlyErrors (e:_) -> if zIsFinished z then Just $ BndErr e else Nothing
      FoundResult r -> Just $ BndRes r

minStrategy
  :: (AShow v,Ord v,Monad m)
  => k
  -> FreeT
    (ItInit (ExZipper (MinTag p v)) (ZItAssoc (MinTag p v)))
    m
    (k,BndR (MinTag p v))
  -> m (k,BndR (MinTag p v))
minStrategy fin = recur
  where
    recur (FreeT m) = m >>= \case
      Pure a -> trace "Min:Found value" $ return a
      Free f -> trace ("Min:Strategic options: " ++ ashow (ashowItInit f))
        $ case f of
          CmdItInit _it ini -> recur ini
          CmdIt it -> recur $ it $ popMinAssocList
          CmdInit ini -> recur ini
          CmdFinished (ExZipper x) -> return
            (fin,fromMaybe (undefined) $ zFullResultMin x)


minTest :: IO (BndR (MinTag TestParams Integer))
minTest =
  fmap (fst . fst)
  $ (`runReaderT` mempty)
  $ (`runFixStateT` def)
  $ runWriterT
  $ do
    putMech 1 $ incrTill "1" ((+ 1),id) 3
    putMech 2 $ incrTill "2" ((+ 1),id) 1
    putMech 3 $ incrTill "3" ((+ 1),id) 2
    let insTrail k tr =
          if k `IS.member` tr
          then Left $ ErrCycle k tr else Right $ IS.insert k tr
    let getMech i =
          withTrail (insTrail i) $ getUpdMech (BndErr $ ErrMissing i) i
    res <- runMech (mkProc $ getMech <$> [1,2,3]) def
    lift3 $ putStrLn $ "Result: " ++ ashow res
    return res
