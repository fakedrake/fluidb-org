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
module Control.Antisthenis.Minimum
  (minTest,MinTag) where

import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.Test
import           Control.Antisthenis.Types
import           Control.Antisthenis.VarMap
import           Control.Antisthenis.Zipper
import           Control.Applicative
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.Writer
import           Control.Utils.Free
import           Data.Foldable
import qualified Data.IntSet                        as IS
import qualified Data.List.NonEmpty                 as NEL
import           Data.Maybe
import           Data.Proxy
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.FixState
import           Data.Utils.Functors
import           Data.Utils.Monoid
import           Data.Utils.Tup
import           GHC.Generics

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
  = SecConcrete (ZRes w)
  | SecSoft (ZBnd w)
  | NoSec
  deriving Generic
instance (AShow (ZRes w),AShow (ZBnd w))
  => AShow (SecondaryBound w)
malMinBound
  :: forall f w a .
  (Ord (ZBnd w),Foldable f,Functor f,Num (ZBnd w))
  => (ZRes w -> ZBnd w -> Bool)
  -> MinAssocList f w a
  -> SecondaryBound w
malMinBound lessThan mal = case malConcrete mal of
  OnlyErrors _ -> maybe NoSec SecSoft elemBnd
  FoundResult firstRes -> case (\b -> (b,lessThan firstRes b)) <$> elemBnd of
    Just (_bnd,True) -> SecConcrete firstRes -- the first result is better than the best case of
                                           -- the next.
    Just (bnd,False) -> SecSoft bnd
    Nothing          -> SecConcrete firstRes -- there are no other results.
  where
    elemBnd = foldl' go Nothing $ fst <$> malElements mal
      where
        go :: Maybe (ZBnd w) -> ZBnd w -> Maybe (ZBnd w)
        go Nothing r   = Just r
        go (Just r0) r = Just $ min r0 r

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


barrel :: NEL.NonEmpty a -> NEL.NonEmpty (NEL.NonEmpty a)
barrel x@(a0 NEL.:| as0) = x NEL.:| go [] a0 as0
  where
    go _pref _a []    = []
    go pref a (a':as) = (a' NEL.:| (a : pref)) : go (a : pref) a' as

minimumOn :: Ord b => NEL.NonEmpty (b,a) -> a
minimumOn ((b,a) NEL.:| as) =
  snd
  $ foldl' (\(b0,a0) (b1,a1) -> if b0 > b1 then (b1,a1) else (b0,a0)) (b,a) as

-- | Return the minimum of the list.
popMinAssocList
  :: (Ord (ZBnd w),AShowV (ZBnd w))
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
  type ZBnd (MinTag p a) = Min' a
  type ZRes (MinTag p a) = Min' a

instance (AShow a
         ,Eq a
         ,Ord a
         ,Num a
         ,ExtParams p
         ,NoArgError (ExtError p)
         ,AShow (ExtError p)) => ZipperParams (MinTag p a) where
  type ZCap (MinTag p a) = Min' a
  type ZEpoch (MinTag p a) = ExtEpoch p
  type ZCoEpoch (MinTag p a) = ExtCoEpoch p
  type ZCoEpoch (MinTag p a) = ExtCoEpoch p
  type ZPartialRes (MinTag p a) = ()
  type ZItAssoc (MinTag p a) =
    MinAssocList [] (MinTag p a)
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = minEvolutionControl
     ,evolutionStrategy = minStrategy
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
  -- | The secondary bound becomes the cap when it is smaller than the
  -- global cap.
  --
  -- Also handles the combination of epoch and coepoch that dictates
  -- whether we must reset.
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ case (secondaryBound,confCap conf) of
      (NoSec,_) -> conf
      (_,CapStruct i) -> conf { confCap = CapStruct $ i - 1 }
      (SecConcrete res,CapVal c) -> if res
        <= c then error "the secondary bound should be smaller than the cap"
        else conf { confCap = CapVal $ min res c }
      (SecSoft bnd,CapVal c) -> conf { confCap = CapVal $ min bnd c }
      (SecConcrete res,ForceResult) -> conf { confCap = CapVal res }
      (SecSoft bnd,ForceResult) -> conf { confCap = CapVal bnd }
    where
      secondaryBound = malMinBound (<) $ bgsIts $ zBgState z

-- | Given a proposed solution and the configuration from which it
-- came return a bound that matches the configuration or Nothing if
-- the zipper should keep evolving to get to a proper resilt.
minEvolutionControl
  :: forall v p r x .
  (Ord v,AShow v,AShow (ExtError p),Num v)
  => Conf (MinTag p v)
  -> Zipper' (MinTag p v) Identity r x
  -> Maybe (BndR (MinTag p v))
minEvolutionControl conf z = case confCap conf of
  CapStruct i -> if i >= 0 then res else res <|> Just (BndBnd 0)
  ForceResult -> res >>= \case
    BndBnd _bnd -> Nothing
    x           -> return x
  CapVal cap -> res >>= \case
    BndBnd bnd -> if cap < bnd then Just $ BndBnd bnd else Nothing
    x          -> return x
  where
    -- The result so far. The cursor is assumed to always be either an
    -- init or the most promising.
    res = case fst3 (runIdentity $ zCursor z) of
      Just r -> Just $ BndBnd r
      Nothing -> case malMinBound (<) $ bgsIts $ zBgState z of
        NoSec         -> Nothing
        SecConcrete r -> Just $ BndRes r
        SecSoft b     -> Just $ BndBnd b

minBndR :: Ord v => BndR (MinTag p v) -> BndR (MinTag p v) -> BndR (MinTag p v)
minBndR = curry $ \case
  (_,e@(BndErr _))    -> e
  (e@(BndErr _),_)    -> e
  (BndBnd a,BndBnd b) -> BndBnd $ min a b
  (BndRes a,BndRes b) -> BndRes $ min a b
  (BndRes a,BndBnd b) -> if a < b then BndRes a else BndBnd b
  (BndBnd a,BndRes b) -> if a < b then BndBnd a else BndRes b


zIsFinished :: Zipper' (MinTag p v) f r x -> Bool
zIsFinished z =
  null (bgsInits $ zBgState z)
  && isNothing (acNonEmpty $ bgsIts $ zBgState z)

-- | Turn the zipper into a value.
--
-- XXX: if the cap is DoNothing we shoudl return the minimum
-- bound. even if Nothing is encountered.
zFullResultMin
  :: (Foldable f,AShow v,Ord v)
  => Zipper' (MinTag p v) f r x
  -> Maybe (BndR (MinTag p v))
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

minStrategy
  :: (AShow v,Ord v,Monad m,AShow (ExtError p))
  => k
  -> FreeT
    (ItInit (ExZipper (MinTag p v)) (ZItAssoc (MinTag p v)))
    m
    (k,BndR (MinTag p v))
  -> m (k,BndR (MinTag p v))
minStrategy fin = recur
  where
    recur (FreeT m) = m >>= \case
      Pure a -> return a
      Free f -> case f of
        CmdItInit _it ini -> recur ini
        CmdIt it -> recur $ it popMinAssocList
        CmdInit ini -> recur ini
        CmdFinished (ExZipper x) -> return
          (fin,fromMaybe undefined $ zFullResultMin x)

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
