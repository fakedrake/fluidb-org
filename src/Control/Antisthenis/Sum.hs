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
  type ZBnd (SumTag p v) = Min v
  type ZRes (SumTag p v) = Sum v

instance (Ord v,Num v,ExtParams p,AShow (ExtEpoch p),AShow (ExtCoEpoch p))
  => ZipperParams (SumTag p v) where
  type ZEpoch (SumTag p v) = ExtEpoch p
  type ZCoEpoch (SumTag p v) = ExtCoEpoch p
  type ZCap (SumTag p v) = Min v
  type ZPartialRes (SumTag p v) =
    Either (ZErr (SumTag p v)) (ZBnd (SumTag p v))
  type ZItAssoc (SumTag p v) =
    SimpleAssoc [] (ZBnd (SumTag p v))
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = sumEvolutionControl
     ,evolutionStrategy = sumEvolutionStrategy
     ,evolutionEmptyErr = error "No arguments provided"
    }
  putRes newBnd (partialRes,newZipper) =
    (\() -> add <$> partialRes <*> toEither newBnd) <$> newZipper
    where
      add = curry $ \case
        (Min Nothing,x) -> x
        (x,Min Nothing) -> x
        (x,y) -> x + y
      toEither = \case
        BndBnd v -> Right v
        BndRes (Sum v) -> Right (Min v)
        BndErr e -> Left e
  replaceRes oldBnd newBnd (oldRes,newZipper) =
    Just $ putRes newBnd ((\x -> x - oldBnd) <$> oldRes,newZipper)
  zLocalizeConf coepoch conf z =
    extCombEpochs (Proxy :: Proxy p) coepoch (confEpoch conf)
    $ conf { confCap = newCap }
    where
      newCap = case zRes z of
        Left _ -> WasFinished
        Right r -> case confCap conf of
          Cap c -> Cap $ c - r
          x -> x

-- | Return the result expected or Nothing if there is more evolving
-- that needs to happen. This can only return bounds.
sumEvolutionControl
  :: Ord v
  => GConf (SumTag p v)
  -> Zipper (SumTag p v) (ArrProc (SumTag p v) m)
  -> Maybe (BndR (SumTag p v))
sumEvolutionControl conf z = case zRes z of
  Left e -> Just $ BndErr e
  Right bound -> case confCap conf of
    Cap cap -> if cap < bound then Just $ BndBnd bound else Nothing
    MinimumWork -> Just $ BndBnd bound
    DoNothing -> Just $ BndBnd bound
    WasFinished -> error "unreachable"
    ForceResult -> Nothing

sumEvolutionStrategy
  :: Monad m
  => x
  -> FreeT
    (ItInit (ExZipper (SumTag p v)) (SimpleAssoc [] (Min v)))
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
        CmdFinished (ExZipper z) -> return
          (fin,either BndErr (\(Min x) -> BndRes $ Sum x) $ zRes z)


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
