{-# LANGUAGE Arrows                 #-}
{-# LANGUAGE CPP                    #-}
{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE DeriveFoldable         #-}
{-# LANGUAGE DeriveFunctor          #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE PatternSynonyms        #-}
{-# LANGUAGE QuantifiedConstraints  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-missing-pattern-synonym-signatures #-}
{-# LANGUAGE TypeApplications       #-}

module Control.Antisthenis.Types
  (IndexErr(..)
  ,ashowItInit
  ,runArrProc
  ,ashowRes
  ,zipperShape
  ,mapCursor
  ,RstCmd
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
  ,AShowW
  ,incrZipperUId,zNamed,trZ) where

import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Moore
import           Control.Antisthenis.ATL.Transformers.Writer
import           Control.Antisthenis.AssocContainer
import           Control.Antisthenis.ZipperId
import           Control.Arrow                               hiding (first,
                                                              second)
import           Control.Monad.Identity
import           Control.Utils.Free
import           Data.Bifunctor
import           Data.Either
import           Data.List
import           Data.Proxy
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Debug
import           Data.Utils.Default
import           Data.Utils.OptSet
import           GHC.Generics

data Cap b
  = CapVal b -- Cap val
  | ForceResult
  deriving (Show,Functor,Generic)
instance AShow b => AShow (Cap b)

type InitProc a = a
type ItProc a = a
type CoitProc a = a

-- | Error related to indexes
data IndexErr i
  = ErrMissing i
  | ErrCycleEphemeral { ecCur :: i }
  | ErrCycle { ecPred :: OptSet i,ecCur :: i }
  | NoArguments
  deriving Generic
instance (AShow i,AShow (OptSet i))
  => AShow (IndexErr i)
type Err = IndexErr Int

-- | Provide a meaningful reset command.
newtype ResetCmd a = DoReset a deriving Functor
type RstCmd w m x = (ResetCmd (FreeT (Cmds w) m x))

-- Existential zipper. This zipper does not have a cursor, it just
-- remembers the structure of the zipper.
data ExZipper w = forall p . ExZipper { runExZipper :: FinZipper w p }
data MayReset a = DontReset a | ShouldReset deriving (Generic,Functor)
instance AShow a => AShow (MayReset a)
type Cmds w = Cmds' (ExZipper w) (ZItAssoc w)
data Cmds' r f a =
  Cmds { cmdReset :: ResetCmd a,cmdItCoit :: ItInit r f a }
  deriving Functor
type ItProcF f a =
  ((forall x . AssocContainer f => NonEmptyAC f x -> (KeyAC f,x,f x))
   -> ItProc a)
data ItInit r f a
  = CmdItInit (ItProcF f a) (InitProc a)
  | CmdIt (ItProcF f a)
  | CmdInit (InitProc a)
  | CmdFinished r -- when a process finishes it should stick to a
                  -- value until the epoch/coepoch pair requests a
                  -- reset.

ashowItInit :: ItInit r f a -> SExp
ashowItInit = \case
  CmdItInit _ _ -> sexp "CmdItInit" [Sym "<proc>",Sym "<proc>"]
  CmdIt _       -> sexp "CmdIt" [Sym "<proc>"]
  CmdInit _     -> sexp "CmdInit" [Sym "<proc>"]
  CmdFinished _ -> sexp "CmdFinished" [Sym "<res>"]
instance Functor (ItInit r f) where
  fmap f = \case
    CmdItInit x y -> CmdItInit (\pop -> f $ x pop) $ f y
    CmdIt x       -> CmdIt $ \pop -> f $ x pop
    CmdInit x     -> CmdInit $ f x
    CmdFinished r -> CmdFinished r

type AShowW w =
  (AShowBndR w
  ,AShow (ZPartialRes w)
  ,Functor (ZItAssoc w)
  ,BndRParams w
  ,AShow (ZEpoch w)
  ,AShow (ZCap w)
  ,AShow (ZItAssoc w ((),())))

-- | Some functions that require shared variables.
data ZProcEvolution w m k =
  ZProcEvolution
  { -- Evolution control decides when to return values and when to
    -- continue.
    evolutionControl :: GConf w -> Zipper w (ArrProc w m) -> Maybe (BndR w)
    -- Evolution strategy decides which branches to take when
    -- evolving. Get the final.
   ,evolutionStrategy
      :: forall x .
      FreeT (Cmds w) m x
      -> m (Maybe (RstCmd w m x),Either (ZCoEpoch w,BndR w) x)
     -- Empty error is the error emitted when there are no arguments
     -- to an operator.
   ,evolutionEmptyErr :: ZErr w
  }

class BndRParams w where
  type ZErr w :: *

  type ZBnd w :: *

  type ZRes w :: *

  bndLt :: Proxy w -> ZBnd w -> ZBnd w -> Bool
  exceedsCap :: Proxy w -> ZCap w -> ZBnd w -> Bool

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
      ,AShow (ZCoEpoch w)
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
  -- one to be propagated to the next argument process.
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
  Conf
  { confCap :: Cap (ZCap w),confEpoch :: ZEpoch w,confTrPref :: () }
  deriving Generic
instance (AShow (ZEpoch w),AShow (ZCap w))
  => AShow (Conf w)
instance Default (ZEpoch w) => Default (Conf w) where
  def = Conf { confCap = ForceResult,confEpoch = def,confTrPref = () }
type GConf w = Conf w
type LConf w = Conf w

-- The it container
data ZipState w a =
  ZipState
  { bgsInits :: [InitProc a]
   ,bgsIts   :: ZItAssoc w (InitProc a,ItProc a)
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
   ,zCursor  :: cursf (Maybe (ZBnd w),InitProc p,p)
   ,zRes     :: pr -- The result without the cursor.
   ,zId      :: ZipperId
  }
  deriving Generic
instance Functor (Zipper' w cursf p) where
  fmap f Zipper {..} =
    Zipper { zCursor = zCursor,zBgState = zBgState,zRes = f zRes,zId = zId }
zNamed :: Zipper'  w cursf p pr -> String -> Bool
zNamed z  = zidNamed (zId z)

lengthZBgState :: Foldable (ZItAssoc w) => ZipState w p -> (Int,Int,(Int,Int))
lengthZBgState ZipState {..} =
  (length bgsInits
  ,length bgsIts
  ,bimap length length $ partition (isLeft . fst) bgsCoits)

data ZShape =
  ZShape
  { zshInits :: Int
   ,zshIts   :: Int
   ,zshCoits :: (Int,Int)
   ,zshCurs  :: Int
   ,zshId    :: ZipperId
  }
  deriving Generic

instance AShow ZShape

zipperShape
  :: (Foldable (ZItAssoc w),Foldable cursf) => Zipper' w cursf p pr -> ZShape
zipperShape z =
  ZShape
  { zshInits = ini
   ,zshIts = it
   ,zshCoits = coit
   ,zshCurs = length (zCursor z)
   ,zshId = zId z
  }
  where
    (ini,it,coit) = lengthZBgState (zBgState z)

instance (AShow (ZItAssoc w (p,p))
         ,AShowBndR w
         ,AShowV p
         ,AShow pr
         ,AShow (f (Maybe (ZBnd w),InitProc p,p)))
  => AShow (Zipper' w f p pr)

-- | A zipper without a cursor.
type FinZipper w p = Zipper' w (Const (ZCoEpoch w)) p (ZPartialRes w)
type Zipper w p = Zipper' w Identity p (ZPartialRes w)
instance (Functor f,Functor (ZItAssoc w)) => Bifunctor (Zipper' w f) where
  bimap f g Zipper {..} =
    Zipper
    { zBgState = fmap f zBgState
     ,zCursor = (\(a,b,c) -> (a,f b,f c)) <$> zCursor
     ,zRes = g zRes
     ,zId = zId
    }

incrZipperUId
  :: Foldable (ZItAssoc w)
  => Zipper' w Identity p pr
  -> Zipper' w Identity p pr
















incrZipperUId z =
  z { zId = (zId z) { zidVersion = (+ 1) <$> zidVersion (zId z) } }



class (Monoid (ExtCoEpoch p)) => ExtParams w p where
  type ExtEpoch p :: *

  type ExtCoEpoch p :: *

  type ExtCap p :: *

  type ExtError p :: *

  type MechVal p :: *

  extExceedsCap :: Proxy (p,w) -> ExtCap p -> ZBnd w -> Bool
  extCombEpochs
    :: Proxy p -> ExtCoEpoch p -> ExtEpoch p -> Conf w -> MayReset (Conf w)

class NoArgError e where
  noArgumentsError :: e

instance NoArgError (IndexErr i) where
  noArgumentsError = NoArguments

pattern ArrProc c = MealyArrow (WriterArrow (Kleisli c))
runArrProc :: ArrProc w m -> LConf w -> m (ZCoEpoch w,(ArrProc w m,BndR w))
runArrProc (ArrProc p) conf = p conf
runArrProc _ _              = error "unreachable"
ashowRes :: (ZRes w -> SExp) -> BndR w -> String
ashowRes ashow'' = ashow . \case
  BndRes r ->  ashow'' r
  BndErr _ -> Sym "<error-result>"
  BndBnd _ -> Sym "<bound-result>"

mapCursor :: (forall a . f a -> g a) -> Zipper' w f p r -> Zipper' w g p r
mapCursor f Zipper {..} =
  Zipper
  { zBgState = zBgState,zCursor = f zCursor,zRes = zRes,zId = zId }

trZ :: (Monad m,AShow a)
    => String
    -> Zipper' w cursf p pr
    -> [String]
    -> a
    -> m ()
trZ msg z fltr d =
  when (null fltr || any (zNamed z) fltr)
  $ (msg ++ "(" ++ ashow (zId z) ++ ")") <<: d
