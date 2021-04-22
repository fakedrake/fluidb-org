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
  (minTest) where

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

-- Ideally this should be a heap but for now this will not grow beyond
-- 10 elements so we are good.
data MinAssocList f w a =
  MinAssocList
  { malElements :: (f (ZBnd w,a))
   ,malConcrete :: Maybe (Either (ZErr w) (ZRes w))
  }
  deriving (Generic,Functor)
instance (AShow (f (ZBnd w,a)),AShowBndR w,AShow a)
  => AShow (MinAssocList f w a)

malBound :: (Ord (ZBnd w),Foldable f) => MinAssocList f w a -> Maybe (ZBnd w,a)
malBound = foldl' go Nothing . malElements
  where
    go Nothing x = Just x
    go (Just a'@(v',_)) (a@(v,_)) = Just $ if v < v' then a else a'

zModIts
  :: (ZItAssoc w ~ MinAssocList [] w)
  => (Maybe (Either (ZErr w) (ZRes w)) -> Maybe (Either (ZErr w) (ZRes w)))
  -> Zipper' w f p pr
  -> Zipper' w f p pr
zModIts f z = z { zBgState = zsModIts $ zBgState z }
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
  acEmpty = MinAssocList [] Nothing
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

instance (Monad m,ZipperParams (Min a))
  => ZipperMonad (Min a) m where
  zCmpEpoch _ a b = return $ a < b

instance BndRParams (Min a) where
  type ZErr (Min a) = Err
  type ZBnd (Min a) = Min a
  type ZRes (Min a) = Min a

instance (AShow a,Eq a,Ord a) => ZipperParams (Min a) where
  type ZCap (Min a) = Min a
  type ZEpoch (Min a) = Int
  type ZPartialRes (Min a) = ()
  type ZItAssoc (Min a) = MinAssocList [] (Min a)
  zprocEvolution =
    ZProcEvolution
    { evolutionControl =
        (\conf z -> deriveOrdSolution conf =<< zFullResultMin z)
     ,evolutionStrategy = minStrategy
     ,evolutionEmptyErr = NoArguments
    }
  -- We only need to do anything if the result is concrete. A
  -- non-error concrete result in combined with the result so far. In
  -- the case where the result is an error the error is
  -- updated. Bounded results are already stored in the associative
  -- structure.

  putRes newBnd ((),newZipper) = case newBnd of
    BndRes r -> zModIts (maybe (Just $ Right r) (Just . fmap (<> r))) newZipper
    BndErr e -> zModIts (const $ Just $ Left e) newZipper
    BndBnd _ -> newZipper

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
  localizeConf conf z = case malConcrete $ bgsIts $ zBgState z of
    Just (Left _e) -> conf { confCap = WasFinished }
    Just (Right concreteBnd) -> case malBound $ bgsIts $ zBgState z of
      Just (minBnd,_) -> conf { confCap = Cap $ concreteBnd <> minBnd }
      Nothing -> conf { confCap = Cap concreteBnd }
    Nothing -> case malBound $ bgsIts $ zBgState z of
      Just (minBnd,_) -> conf { confCap = Cap minBnd }
      Nothing -> conf { confCap = MinimumWork }


-- | Given a solution and the configuration return a bound that
-- matches the configuration or Nothing if the zipper should keep
-- evolving to get to a proper resilt.
deriveOrdSolution
  :: forall v .
  (Ord v,AShow v)
  => Conf (ZBnd (Min v))
  -> BndR (Min v)
  -> Maybe (BndR (Min v))
deriveOrdSolution conf res = case confCap conf of
  WasFinished -> Just $ BndErr undefined
  DoNothing -> Just res
  MinimumWork -> Just res
  ForceResult -> case res of
    BndBnd _bnd -> Nothing
    BndErr _e -> Nothing
    x -> return x
  Cap cap -> case res of
    BndBnd bnd -> if bnd <= cap then Nothing else Just $ BndBnd bnd
    x -> return x

minBndR :: Ord v => BndR (Min v) -> BndR (Min v) -> BndR (Min v)
minBndR = curry $ \case
  (_,e@(BndErr _)) -> e
  (e@(BndErr _),_) -> e
  (BndBnd a,BndBnd b) -> BndBnd $ min a b
  (BndRes a,BndRes b) -> BndRes $ min a b
  (BndRes a,BndBnd b) -> if a < b then BndRes a else BndBnd b
  (BndBnd a,BndRes b) -> if a < b then BndBnd a else BndRes b


-- This turns the zipper into a value. However at the end of the
-- computation the zipper is actually.
zFullResultMin
  :: (Foldable f,AShow v,Ord v)
  => Zipper' (Min v) f r p
  -> Maybe (BndR (Min v))
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
      Nothing -> Nothing
      Just (Left e) -> Just $ BndErr e
      Just (Right x) -> Just $ BndRes x

minStrategy
  :: (AShow v,Ord v,Monad m)
  => k
  -> FreeT (ItInit (ExZipper (Min v)) (ZItAssoc (Min v))) m (k,BndR (Min v))
  -> m (k,BndR (Min v))
minStrategy fin = recur
  where
    recur (FreeT m) = m >>= \case
      Pure a -> return a
      Free f -> case f of
        CmdItInit _it ini -> recur ini
        CmdIt it -> recur $ it $ popMinAssocList
        CmdInit ini -> recur ini
        CmdFinished (ExZipper x) -> return
          (fin,fromMaybe (undefined) $ zFullResultMin x)

minTest :: IO (BndR (Min Integer))
minTest = fmap fst $ (`runReaderT` mempty) $ (`runFixStateT` def) $ do
  putMech 1 $ incrTill "1" ((+ 1),id) $ Cap 3
  putMech 2 $ incrTill "2" ((+ 1),id) $ Cap 1
  putMech 3 $ incrTill "3" ((+ 1),id) $ Cap 2
  let insTrail k tr =
        if k `IS.member` tr
        then Left $ ErrCycle k tr else Right $ IS.insert k tr
  let getMech i = withTrail (insTrail i) $ getUpdMech (BndErr $ ErrMissing i) i
  res <- runMech (mkProc $ getMech <$> [1,2,3]) def
  lift2 $ putStrLn $ "Result: " ++ ashow res
  return res
