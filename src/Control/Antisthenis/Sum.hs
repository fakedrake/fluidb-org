{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}
module Control.Antisthenis.Sum (SumTag) where

import Data.Utils.Tup
import Control.Monad.Identity
import GHC.Generics
import Data.Utils.Default
import Data.Utils.AShow
import Data.Proxy
import Control.Monad.Trans.Free
import Control.Antisthenis.Types
import Data.Utils.Monoid
import Control.Antisthenis.AssocContainer

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
  SumPart f <*> SumPart x = SumPart $ f x
  SumPart _ <*> SumPartInit = SumPartInit
  SumPart _ <*> SumPartErr e = SumPartErr e
  SumPartErr x <*> _ = SumPartErr x
  SumPartInit <*> _ = SumPartInit
instance Default (SumPartialRes p v a) where
  def = SumPartInit

instance (AShow v
         ,Ord v
         ,Num v
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
      add (SumPart v0) bnd = case bnd of
        BndBnd v -> SumPart $ v + v0 -- will be registered as it.
        BndRes (Sum Nothing) -> SumPart v0
        BndRes (Sum (Just v)) -> SumPart $ (+) v <$> v0
        BndErr e -> SumPartErr e
      toPartial = \case
        BndBnd v -> SumPart v
        BndRes (Sum Nothing) -> SumPartInit
        BndRes (Sum (Just i)) -> SumPart $ Min' i
        BndErr e -> SumPartErr e
  replaceRes oldBnd newBnd (oldRes,newZipper) =
    Just $ putRes newBnd ((\x -> x - oldBnd) <$> oldRes,newZipper)
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ conf { confCap = newCap }
    where
      newCap = case zRes z of
        SumPartErr _ -> CapStruct (-1)
        SumPartInit -> case confCap conf of
          CapStruct s -> CapStruct $ s - 1
          x -> x
        SumPart r -> case confCap conf of
          CapVal c -> CapVal cap'
            where
              cap' =
                maybe
                  (c - r)
                  (\b0 -> c - r + b0)
                  (fst3 $ runIdentity $ zCursor z)
          CapStruct s -> CapStruct $ s - 1
          ForceResult -> ForceResult

-- | Return the result expected or Nothing if there is more evolving
-- that needs to happen. This can only return bounds.
sumEvolutionControl
  :: (Num v,Ord v,AShow v,AShow (ExtError p))
  => GConf (SumTag p v)
  -> Zipper (SumTag p v) (ArrProc (SumTag p v) m)
  -> Maybe (BndR (SumTag p v))
sumEvolutionControl conf z = case zRes z of
  SumPartErr e -> Just $ BndErr e
  SumPartInit -> case confCap conf of
    CapStruct i -> if i < 0 then Nothing else Just $ BndBnd 0
    CapVal bnd -> if bnd < 0 then Just $ BndBnd 0 else Nothing
    _ -> Nothing
  SumPart bound -> case confCap conf of
    CapVal cap -> if cap < bound then Just $ BndBnd bound else Nothing
    CapStruct i -> if
      | i >= 0 -> Nothing
      | otherwise -> Just $ BndBnd bound
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
          SumPartInit -> BndRes $ Sum Nothing
          SumPartErr e -> BndErr e)

#if 0
sumToMin :: Sum v -> Min v
sumToMin (Sum v) = Min v
minToSum :: Min v -> Sum v
minToSum (Min v) = Sum v

sumTest :: IO (BndR (SumTag TestParams Integer))
sumTest =
  fmap (fst . fst)
  $ (`runReaderT` mempty)
  $ (`runFixStateT` def)
  $ runWriterT
  $ do
    putMech 1 $ incrTill "1" ((+ 1),minToSum) 3
    putMech 2 $ incrTill "2" ((+ 1),minToSum) 1
    putMech 3 $ incrTill "3" ((+ 1),minToSum) 2
    res <- runMech (mkProc $ getMech <$> [1,2,3]) def
    lift3 $ putStrLn $ "Result: " ++ ashow res
    return res
  where
    insTrail k tr =
      if k `IS.member` tr then Left $ ErrCycle k tr else Right $ IS.insert k tr
    getMech i = withTrail (insTrail i) $ getUpdMech (BndErr $ ErrMissing i) i
#endif
