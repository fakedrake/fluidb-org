{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
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
{-# OPTIONS_GHC -Wno-missing-pattern-synonym-signatures #-}

module Control.Antisthenis.Types
  (IndexErr(..)
  ,ashowItInit
  ,runArrProc
  ,ashowRes
  ,lengthZ
  ,mapCursor
  ,Err
  ,ArrProc'
  ,NoArgError(..)
  ,ExtParams(..)
  ,Zipper'(..)
  ,ExZipper(..)
  ,ZProcEvolution(..)
  ,Zipper
  ,Cap(..)
  ,Conf(..)
  ,GConf
  ,LConf
  ,ArrProc
  ,pattern ArrProc
  ,Cmds'(..)
  ,Cmds
  ,ItInit(..)
  ,ZipperParams(..)
  ,BndRParams(..)
  ,MayReset(..)
  ,ResetCmd(..)
  ,ZipState(..)
  ,BndR(..)
  ,AShowBndR
  ,InitProc
  ,ItProc
  ,CoitProc
  ,capWasFinished
  ,AShowW) where

import Data.Utils.OptSet
import Data.Proxy
import Control.Antisthenis.ATL.Transformers.Moore
import Control.Antisthenis.ATL.Transformers.Writer
import Data.Bifunctor
import Data.Utils.EmptyF
import Control.Monad.Identity
import Control.Monad.Trans.Free
import Control.Antisthenis.AssocContainer
import Control.Arrow hiding (first,second)
import Control.Antisthenis.ATL.Transformers.Mealy
import GHC.Generics
import Data.Utils.AShow
import Data.Utils.Default

data Cap b
  = CapStruct Int -- Maximum depth NOT max steps
  | CapVal b -- Cap val
  | ForceResult
  deriving (Show,Functor,Generic)
instance AShow b => AShow (Cap b)
capWasFinished :: Cap b -> Bool
capWasFinished (CapStruct i) = i < 0
capWasFinished _ = False

type InitProc a = a
type ItProc a = a
type CoitProc a = a

-- | Error related to indexes
data IndexErr i
  = ErrMissing i
  | ErrCycle i (OptSet i)
  | NoArguments
  deriving Generic
instance (AShow i,AShow (OptSet i))
  => AShow (IndexErr i)
type Err = IndexErr Int

-- | Either provide a meaningful reset command or delegate to a the
-- previous reset branch.
newtype ResetCmd a = DoReset a deriving Functor
-- Existential zipper
data ExZipper w = forall p . ExZipper { runExZipper :: FinZipper w p }
data MayReset a = DontReset a | ShouldReset deriving Functor
type Cmds w = Cmds' (ExZipper w) (ZItAssoc w)
data Cmds' r f a = Cmds { cmdReset :: ResetCmd a,cmdItCoit :: MayReset (ItInit r f a) }
  deriving Functor
type ItProcF f a =
  ((forall x . AssocContainer f => NonEmptyAC f x -> (KeyAC f,x,f x))
   -> ItProc a)
data ItInit r f a
  = CmdItInit (ItProcF f a) (InitProc a)
  | CmdIt (ItProcF f a)
  | CmdInit (InitProc a)
  | CmdFinished r

ashowItInit :: ItInit r f a -> SExp
ashowItInit = \case
  CmdItInit _ _ -> sexp "CmdItInit" [Sym "<proc>",Sym "<proc>"]
  CmdIt _ -> sexp "CmdIt" [Sym "<proc>"]
  CmdInit _ -> sexp "CmdInit" [Sym "<proc>"]
  CmdFinished _ -> sexp "CmdFinished" [Sym "<res>"]
instance Functor (ItInit r f) where
  fmap f = \case
    CmdItInit x y -> CmdItInit (\pop -> f $ x pop) $ f y
    CmdIt x -> CmdIt $ \pop -> f $ x pop
    CmdInit x -> CmdInit $ f x
    CmdFinished r -> CmdFinished r

type AShowW w =
  (AShowBndR w
  ,AShow (ZPartialRes w)
  ,Functor (ZItAssoc w)
  ,BndRParams w
  ,AShow (ZItAssoc w ((),())))

-- | Some functions that require shared variables.
data ZProcEvolution w m k =
  ZProcEvolution
  {
    -- Evolution control decides when to return values and when to
    -- continue.
    evolutionControl :: GConf w -> Zipper w (ArrProc w m) -> Maybe k
    -- Evolution strategy decides which branches to take when
    -- evolving. Get the final.
   ,evolutionStrategy
      :: forall x .
      x
      -> FreeT (ItInit (ExZipper w) (ZItAssoc w)) m (x,k)
      -> m (x,k)
     -- Empty error is the error emitted when there are no arguments
     -- to an operator.
   ,evolutionEmptyErr :: ZErr w
  }

class BndRParams w where
  type ZErr w :: *

  type ZBnd w :: *

  type ZRes w :: *


type Old a = a
type New a = a

-- | This module is highly parametric. To avoid a load of different
-- type variables a zipper implementation is expected to associate it
-- with a particular witness type. This way we can define conversion
-- between witness types so that operators can communicate.
type Local a = a
class (KeyAC (ZItAssoc w) ~ ZBnd w
      ,BndRParams w
      ,Monoid (ZCoEpoch w)
      ,AssocContainer (ZItAssoc w)
      ,Default (ZPartialRes w)) => ZipperParams w where
  type ZCap w :: *

  type ZEpoch w :: *

  type ZCoEpoch w :: *

  type ZItAssoc w :: * -> *

  type ZPartialRes w :: *

  zprocEvolution :: Monad m => ZProcEvolution w m (BndR w)

  -- | Insert the result of an init. This result does not replace a
  -- previous one. It is completely new
  putRes :: Foldable f
         => BndR w
         -> (ZPartialRes w,Zipper' w f p ())
         -> Zipper' w f p (ZPartialRes w)

  -- | Insert the result of an iteration after removing the previous
  -- result it yielded.
  replaceRes
    :: Foldable f
    => Old (Local (ZBnd w))
    -> Old (Local (BndR w))
    -> (Old (ZPartialRes w),New (Zipper' w f p ()))
    -> New (Maybe (Zipper' w f p (ZPartialRes w)))

  -- | From the configuration that is global to the op make the local
  -- one to be propagated to the next argument process.β
  zLocalizeConf :: ZCoEpoch w -> GConf w -> Zipper w p -> MayReset (LConf w)

data BndR w
  = BndErr (ZErr w)
  | BndRes (ZRes w)
  | BndBnd (ZBnd w)
  deriving Generic

instance (Eq (ZErr w),Eq (ZRes w),Eq (ZBnd w)) => Eq (BndR w) where
  a == b = from a == from b

type AShowBndR w = (BndRParams w,AShow (ZBnd w),AShow (ZRes w),AShow (ZErr w))
instance AShowBndR w => AShow (BndR w)

type ArrProc w m =
  MealyArrow (WriterArrow (ZCoEpoch w) (Kleisli m)) (LConf w) (BndR w)
type ArrProc' w m =
  MooreCat (WriterArrow (ZCoEpoch w) (Kleisli m)) (LConf w) (BndR w)

data Conf w =
  Conf { confCap :: Cap (ZCap w)
        ,confEpoch :: ZEpoch w
        ,confTrPref :: String
       }
  deriving Generic
instance Default (ZEpoch w) => Default (Conf w) where
  def = Conf { confCap = ForceResult,confEpoch = def,confTrPref = "no-pref" }
type GConf w = Conf w
type LConf w = Conf w

-- The it container
data ZipState w a =
  ZipState
  { bgsInits :: [InitProc a]
   ,bgsIts :: ZItAssoc w (InitProc a,ItProc a)
   ,bgsCoits :: [(Either (ZErr w) (ZRes w),CoitProc a)]
  } deriving Generic
instance (AShow (ZItAssoc w (a,a)),AShowBndR w,AShowV a)
  => AShow (ZipState w a)


instance Functor (ZItAssoc w) => Functor (ZipState w) where
  fmap f ZipState {..} =
    ZipState
    { bgsInits = f <$> bgsInits
     ,bgsIts = bimap f f <$> bgsIts
     ,bgsCoits = fmap (fmap f) bgsCoits
    }

data Zipper' w cursf (p :: *) pr =
  Zipper
  { zBgState :: ZipState w p
    -- The cursor has NOT been counted in the result. Its the next
    -- element to be ran. The bound provided is for the case that.
   ,zCursor :: cursf (Maybe (ZBnd w),InitProc p,p)
   ,zRes :: pr -- The result without the cursor.
   ,zId :: String
  }
  deriving Generic
instance Functor (Zipper' w cursf p) where
  fmap f Zipper {..} =
    Zipper { zCursor = zCursor,zBgState = zBgState,zRes = f zRes,zId = zId }

lengthZBgState :: Foldable (ZItAssoc w) => ZipState w p -> (Int,Int,Int)
lengthZBgState ZipState {..} = (length bgsInits,length bgsIts,length bgsCoits)

data ZLen =
  ZLen
  { zLenInits :: Int
   ,zLenIts :: Int
   ,zLenCoits :: Int
   ,zLenCurs :: Int
   ,zLenId :: String
  }
  deriving Generic

instance AShow ZLen

lengthZ :: (Foldable (ZItAssoc w),Foldable cursf) => Zipper' w cursf p pr -> ZLen
lengthZ z =
  ZLen
  { zLenInits = ini
   ,zLenIts = it
   ,zLenCoits = coit
   ,zLenCurs = length (zCursor z)
   ,zLenId = zId z
  }
  where
    (ini,it,coit) = lengthZBgState (zBgState z)

instance (AShow (ZItAssoc w (p,p)),AShowBndR w,AShowV p,AShow pr)
  => AShow (Zipper' w Identity p pr)

-- | A zipper without a cursor.
type FinZipper w p = Zipper' w EmptyF p (ZPartialRes w)
type Zipper w p = Zipper' w Identity p (ZPartialRes w)
instance (Functor f,Functor (ZItAssoc w)) => Bifunctor (Zipper' w f) where
  bimap f g (Zipper {..}) =
    Zipper
    { zBgState = fmap f zBgState
     ,zCursor = (\(a,b,c) -> (a,f b,f c)) <$> zCursor
     ,zRes = g zRes
     ,zId = zId
    }
class Monoid (ExtCoEpoch p) => ExtParams p where
  type ExtEpoch p :: *

  type ExtCoEpoch p :: *

  type ExtError p :: *

  extCombEpochs
    :: Proxy p -> ExtCoEpoch p -> ExtEpoch p -> Conf w -> MayReset (Conf w)

class NoArgError e where
  noArgumentsError :: e

instance NoArgError (IndexErr i) where
  noArgumentsError = NoArguments

pattern ArrProc c = MealyArrow (WriterArrow (Kleisli c))
runArrProc :: ArrProc w m -> LConf w -> m (ZCoEpoch w,(ArrProc w m,BndR w))
runArrProc (ArrProc p) conf = p conf
runArrProc _ _ = error "unreachable"
ashowRes :: (ZRes w -> SExp) -> BndR w -> String
ashowRes ashow'' = ashow . \case
  BndRes r ->  ashow'' r
  BndErr _ -> Sym "<error>"
  BndBnd _ -> Sym "<bound>"

mapCursor :: (forall a . f a -> g a) -> Zipper' w f p r -> Zipper' w g p r
mapCursor f Zipper {..} =
  Zipper { zBgState = zBgState,zCursor = f zCursor,zRes = zRes,zId = zId }
