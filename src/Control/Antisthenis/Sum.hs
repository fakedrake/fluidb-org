{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveGeneric #-}
module Control.Antisthenis.Sum () where

import Data.Utils.Debug
import Data.Proxy
import Control.Monad.Reader
import Data.Utils.Default
import Data.Utils.AShow
import Data.Utils.Functors
import Control.Antisthenis.Test
import Control.Antisthenis.VarMap
import qualified Data.IntSet as IS
import Control.Monad.Trans.Free
import Control.Antisthenis.Types
import Data.Utils.Monoid
import Control.Antisthenis.Zipper
import Control.Antisthenis.AssocContainer

-- | Addition does not have an absorbing element. Therefore all
-- elements need to be evaluated to get a rigid result. However it is
-- often the case in antisthenis that a computation does not stop at a
-- final result but when a lower bound is met. The ideal case would be
-- to have a bound derivative and to move in the most promising side
-- but we do not so we will follow the greedy approach of finishing
-- with each element in sequence.

instance BndRParams (Sum v) where
  type ZErr (Sum v) = Err
  type ZBnd (Sum v) = Min v
  type ZRes (Sum v) = Sum v

instance (Ord v,Num v,AShow v,Monad m)
  => ZipperMonad (Sum v) m where
  zCmpEpoch Proxy x y = return $ x < y

instance (Ord v,Num v,AShow v) => ZipperParams (Sum v) where
  type ZEpoch (Sum v) = Int
  type ZCap (Sum v) = Min v
  type ZPartialRes (Sum v) =
    Either (ZErr (Sum v)) (ZBnd (Sum v))
  type ZItAssoc (Sum v) = SimpleAssoc [] (ZBnd (Sum v))
  zprocEvolution =
    ZProcEvolution
    { evolutionControl = sumEvolutionControl
     ,evolutionStrategy = sumEvolutionStrategy
     ,evolutionEmptyErr = error "No arguments provided"
    }
  putRes newBnd (partialRes,newZipper) =
    trace ("putRes: " ++ ashow (newBnd,partialRes))
    $ (\() -> add <$> partialRes <*> toEither newBnd) <$> newZipper
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
    trace ("replace: " ++ ashow (oldBnd,newBnd,oldRes))
    $ Just
    $ putRes newBnd ((\x -> x - oldBnd) <$> oldRes,newZipper)
  localizeConf conf z = conf { confCap = case zRes z of
    Left _ -> WasFinished
    Right r -> case confCap conf of
      Cap c -> Cap $ c - r
      x -> x }

-- | Return the result expected or Nothing if there is more evolving
-- that needs to happen. This can only return bounds.
sumEvolutionControl
  :: Ord v
  => GConf (Sum v)
  -> Zipper (Sum v) (ArrProc (Sum v) m)
  -> Maybe (BndR (Sum v))
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
    (ItInit (ExZipper (Sum v)) (SimpleAssoc [] (Min v)))
    m
    (x,BndR (Sum v))
  -> m (x,BndR (Sum v))
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

sumToMin :: Sum v -> Min v
sumToMin (Sum v) = Min v
minToSum :: Min v -> Sum v
minToSum (Min v) = Sum v

sumTest :: IO (BndR (Sum Integer))
sumTest = fmap fst $ (`runReaderT` mempty) $ (`runFixStateT` def) $ do
  putMech 1 $ incrTill "1" ((+ 1),minToSum) $ Cap 3
  putMech 2 $ incrTill "2" ((+ 1),minToSum) $ Cap 1
  putMech 3 $ incrTill "3" ((+ 1),minToSum) $ Cap 2
  let insTrail k tr =
        if k `IS.member` tr
        then Left $ ErrCycle k tr else Right $ IS.insert k tr
  let getMech i = withTrail (insTrail i) $ getUpdMech (BndErr $ ErrMissing i) i
  res <- runMech (mkProc $ getMech <$> [1,2,3]) def
  lift2 $ putStrLn $ "Result: " ++ ashow res
  return res
