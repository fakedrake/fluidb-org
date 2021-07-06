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
import           Control.Antisthenis.Types
import           Control.Monad.Identity
import           Control.Utils.Free
import           Data.Proxy
import           Data.Utils.AShow
import           Data.Utils.Default
import           Data.Utils.Monoid
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
  type ZBnd (SumTag p v) = Min' v
  type ZRes (SumTag p v) = Sum v

data SumPartialRes p v a
  = SumPartErr (ZErr (SumTag p v))
  | SumPart a
  | SumPartInit
  deriving (Generic,Functor)
instance (AShow a,AShow (ZErr (SumTag p v)))
  => AShow (SumPartialRes p v a)
instance Applicative (SumPartialRes p v) where
  pure = SumPart
  SumPart f <*> SumPart x    = SumPart $ f x
  SumPart _ <*> SumPartInit  = SumPartInit
  SumPart _ <*> SumPartErr e = SumPartErr e
  SumPartErr x <*> _         = SumPartErr x
  SumPartInit <*> _          = SumPartInit
instance Default (SumPartialRes p v a) where
  def = SumPartInit

instance (AShow v
         ,Ord2 v v
         ,Invertible v
         ,ExtParams p
         ,AShow (ExtError p)
         ,AShow (ExtEpoch p)
         ,AShow (ExtCoEpoch p)) => ZipperParams (SumTag p v) where
  type ZEpoch (SumTag p v) = ExtEpoch p
  type ZCoEpoch (SumTag p v) = ExtCoEpoch p
  type ZCap (SumTag p v) = Min' v
  type ZPartialRes (SumTag p v) =
    SumPartialRes p v (ZBnd (SumTag p v))
  type ZItAssoc (SumTag p v) =
    SimpleAssoc [] (ZBnd (SumTag p v))
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = sumEvolutionControl
     ,evolutionStrategy = sumEvolutionStrategy
     ,evolutionEmptyErr = error "No arguments provided"
    }
  putRes newBnd (partialRes,newZipper) =
    (\() -> add partialRes newBnd) <$> newZipper
    where
      add SumPartInit bnd = toPartial bnd
      add e@(SumPartErr _) _ = e
      add (SumPart partRes) bnd = case bnd of
        BndBnd b              -> SumPart $ addMin b partRes -- will be registered as it.
        BndRes (Sum Nothing)  -> SumPart partRes
        BndRes (Sum (Just r)) -> SumPart $ (<>) r <$> partRes
        BndErr e              -> SumPartErr e
      toPartial = \case
        BndBnd v              -> SumPart v
        BndRes (Sum Nothing)  -> SumPartInit
        BndRes (Sum (Just i)) -> SumPart $ Min' i
        BndErr e              -> SumPartErr e
  replaceRes oldBnd newBnd (oldRes,newZipper) =
    Just $ putRes newBnd ((`subMin` oldBnd) <$> oldRes,newZipper)
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ conf { confCap = newCap }
    where
      newCap = case zRes z of
        SumPartErr _ -> CapStruct (-1)
        SumPartInit -> case confCap conf of
          CapStruct s -> CapStruct $ s - 1
          x           -> x
        SumPart r -> case confCap conf of
          CapVal c -> CapVal cap'
            where
              cap' =
                maybe
                  (c `subMin` r)
                  (\b0 -> (c `subMin` r) `addMin` b0)
                  (fst3 $ runIdentity $ zCursor z)
          CapStruct s -> CapStruct $ s - 1
          ForceResult -> ForceResult

subMin :: Invertible v => Min' v -> Min' v -> Min' v
subMin (Min' a) (Min' b) = Min' $ a `imappend` inv b
addMin :: Semigroup v => Min' v -> Min' v -> Min' v
addMin (Min' a) (Min' b) = Min' $ a <> b
-- | Return the result expected or Nothing if there is more evolving
-- that needs to happen. This can only return bounds.
sumEvolutionControl
  :: forall v p m . (Invertible v,Ord2 v v,AShow v,AShow (ExtError p))
  => GConf (SumTag p v)
  -> Zipper (SumTag p v) (ArrProc (SumTag p v) m)
  -> Maybe (BndR (SumTag p v))
sumEvolutionControl conf z = case zRes z of
  SumPartErr e -> Just $ BndErr e
  SumPartInit -> case confCap conf of
    CapStruct i -> ifLt i (0 :: Int) Nothing (Just $ BndBnd $ Min' mempty)
    CapVal bnd
      -> ifLt bnd (Min' mempty :: Min' v) (Just $ BndBnd $ Min' mempty) Nothing
    _ -> Nothing
  SumPart bound -> case confCap conf of
    CapVal cap  -> ifLt cap bound (Just $ BndBnd bound) Nothing
    CapStruct i -> ifLt (0 :: Int) i (Just $ BndBnd bound) Nothing
    ForceResult -> Nothing

sumEvolutionStrategy
  :: Monad m
  => x
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
        CmdFinished (ExZipper z) -> return (fin,case zRes z of
          SumPart (Min' bnd) -> BndRes $ Sum $ Just bnd
          SumPartInit        -> BndRes $ Sum Nothing
          SumPartErr e       -> BndErr e)
