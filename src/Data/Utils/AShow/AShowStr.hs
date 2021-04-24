{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE CPP                    #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE QuantifiedConstraints  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
module Data.Utils.AShow.AShowStr
  (AShowStr(..)
  ,AShowError(..)
  ,HasCallStack
  ,MonadAShowErr
  ,aassert
  ,bimapAShowStr
  ,throwAStr
  ,mkErrAStr
  ,mkAStr
  ,demoteAStr) where

import           Control.Monad.Except
import qualified Data.Constraint         as DC
import           Data.Proxy
import           Data.String
import           Data.Utils.AShow.Common
import           Data.Utils.AShow.Print
import           Data.Void
import           GHC.Generics
import           GHC.Stack

-- | A type that is a union type constructable by (AShowStr e s).
--
-- eg.
--
-- data Err e s = StrErr (AShowStr e s) | Other1 | Other2
--   deriving Generic
-- instance AShowError e s (Err e s) -- automatic with Generic
-- instance (AShowV e, AShowV s) => AShow (Err e s)
--
-- Then we can throw it with `throwAStr` as if (AShowV e, AShowV s)
-- and it's then ashow-able as per the above instance.
class AShowError e s err | err -> e, err -> s where
  fromAShowAStr :: AShowStr e s -> err
  default fromAShowAStr ::
    (Generic err, GAShowError e s (PathToAShowStr (Rep err)) (Rep err)) =>
    AShowStr e s -> err
  fromAShowAStr str =
    to $ gfromAShowStr (Proxy :: Proxy (PathToAShowStr (Rep err))) str

mkAStr :: ((AShowV e, AShowV s) => String) -> AShowStr e s
mkAStr msg = AShowStr $ \d -> DC.withDict d msg

mkErrAStr :: AShowError e s err => ((AShowV e, AShowV s) => String) -> err
mkErrAStr msg = fromAShowAStr $ mkAStr msg

instance AShowError e s (AShowStr e s) where
  fromAShowAStr = id

type MonadAShowErr e s err m =
  (HasCallStack,AShowError e s err,MonadError err m)

throwAStr :: forall e s err m a . MonadAShowErr e s err m =>
            ((AShowV e, AShowV s) => String) -> m a
throwAStr msg =
  throwError $ mkErrAStr $ prettyCallStack callStack ++ "\n--\n" ++ msg

aassert :: forall e s err m . (HasCallStack, AShowError e s err, MonadError err m) =>
          Bool -> ((AShowV e,AShowV s) => String) -> m ()
aassert b msg = unless b $ throwAStr msg

-- AShowStr is NOT readable from it's ashow serialization
newtype AShowStr e s =
  AShowStr { getAShowStr :: DC.Dict (AShowV e, AShowV s) -> String}
  deriving Generic
bimapAShowStr :: ((AShowV e',AShowV s') DC.:- (AShowV e,AShowV s))
              -> AShowStr e s -> AShowStr e' s'
bimapAShowStr sub (AShowStr f) = AShowStr $ \d -> f $ DC.mapDict sub d
instance Monoid (AShowStr e s) where
  mempty = AShowStr $ const mempty
instance Semigroup (AShowStr e s) where
  AShowStr f <> AShowStr g = AShowStr $ \d -> f d <> g d
instance (AShowV e, AShowV s) => AShow (AShowStr e s) where
  ashow' (AShowStr x) = ashow' $ x DC.Dict
instance (AShowV e, AShowV s) => Show (AShowStr e s) where
  show = showSExpOneLine False . ashow'
instance Eq (AShowStr e s) where _ == _ = False
instance IsString (AShowStr e s) where fromString = AShowStr . const
data LBranch f
data RBranch f
data Here
type family ChoseBranch a b where
  ChoseBranch Void Void = Void
  ChoseBranch Void x = RBranch x
  ChoseBranch x y = LBranch x
type family PathToAShowStr a where
  PathToAShowStr (a :+: b) = ChoseBranch (PathToAShowStr a) (PathToAShowStr b)
  PathToAShowStr (M1 i m x) = PathToAShowStr x
  PathToAShowStr (a :*: b) = Void
  PathToAShowStr (Rec0 (AShowStr e s)) = Here
  PathToAShowStr a = Void
class GAShowError e s p f | f -> e, f -> s where
  gfromAShowStr :: Proxy p -> AShowStr e s -> f a
instance GAShowError e s p c => GAShowError e s p (M1 i m c) where
  gfromAShowStr _ str = M1 $ gfromAShowStr (Proxy :: Proxy p) str
instance GAShowError e s Here (Rec0 (AShowStr e s)) where
  gfromAShowStr _ = K1
instance GAShowError e s p b => GAShowError e s (RBranch p) (a :+: b) where
  gfromAShowStr _ str = R1 $ gfromAShowStr (Proxy :: Proxy p) str
instance GAShowError e s p a => GAShowError e s (LBranch p) (a :+: b) where
  gfromAShowStr _ str = L1 $ gfromAShowStr (Proxy :: Proxy p) str

demoteAStr :: forall e s e' s' .
             ((AShowV e, AShowV s) DC.:- (AShowV e', AShowV s'))
           -> AShowStr e' s'
           -> AShowStr e s
demoteAStr inferes (AShowStr f) = AShowStr $ go f where
  go :: (DC.Dict (AShowV e',AShowV s') -> String)
     -> DC.Dict (AShowV e,AShowV s) -> String
  go f' d = f' $ DC.mapDict inferes d
