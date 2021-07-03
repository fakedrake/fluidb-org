{-# LANGUAGE CPP                    #-}
{-# LANGUAGE ConstraintKinds        #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE DeriveGeneric          #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE StandaloneDeriving     #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE TypeSynonymInstances   #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}

module Data.Utils.AShow.Common
  ( AShow(..)
  , SExp(..)
  , AShowList
  , VecType
  , StringVec
  , NormalVec
  , AShowV
  , sexp
  , recSexp
  , genericAShow'
  , ashowCase'
  ) where

import           Data.Functor.Identity
import qualified Data.HashMap.Lazy     as HM
import qualified Data.HashSet          as HS
import qualified Data.IntMap           as IM
import qualified Data.IntSet           as IS
import qualified Data.List.NonEmpty    as NEL
import           Data.Proxy
import           Data.Ratio
import           Data.Semigroup
import qualified Data.Set              as DS
import           Data.Utils.Compose
import           Data.Utils.Const
import           Data.Utils.Functors
import           Data.Utils.Ranges
import           Data.Utils.Tup
import           Data.Void
import           GHC.Generics

data SExp = Sym String
  | Rec String [(String,SExp)]
  | Case [(String,SExp)]
  | Str String
  | Vec [SExp]
  | Tup [SExp]
  | Sub [SExp] deriving (Eq, Show)

-- | We want to be able to override the generic behaviour with our own.
genericAShow' :: (GAShow (Rep a), Generic a) => a -> SExp
genericAShow' = gashow' . from

class GAShow f where
  gashow' :: f a -> SExp

class GSelList f where
  gSelList :: f a -> [(String, SExp)]
instance GSelList U1 where
  gSelList U1 = []
instance (GAShow a, Selector c) => GSelList (M1 S c a) where
  gSelList s@(M1 a) = [(selName s, gashow' a)]
instance (GSelList a, GSelList b) => GSelList (a :*: b) where
  gSelList (a :*: b) = gSelList a ++ gSelList b

-- Ignore dataype information
instance (GAShow a, Datatype c) => GAShow (M1 D c a) where
  gashow' (M1 x) =  gashow' x
instance (GSelList a, Constructor c) => GAShow (M1 C c a) where
  gashow' c@(M1 x) =
    if conIsRecord c
    then recSexp (conName c) $ gSelList x
    else case gSelList x of
      []   -> Sym $ conName c
      args -> sexp (conName c) $ snd <$> args

instance GAShow U1 where
  gashow' U1 = Sub []

-- | Sums: encode choice between constructors
instance (GAShow a, GAShow b) => GAShow (a :+: b) where
  gashow' (L1 x) = gashow' x
  gashow' (R1 x) = gashow' x

instance (AShow a) => GAShow (Rec0 a) where
  gashow' (K1 a) = ashow' a

type AShowV x = (AShow x, AShowList (VecType x) x)

-- ashow :: AShow a => a -> String
-- ashow = printSExp . ashow'
-- Algebra with phantom operators that allow reasoning about internal
-- properties.
-- The primary ashow
class AShow a where
  ashow' :: a -> SExp
  default ashow' :: (Generic a, GAShow (Rep a)) => a -> SExp
  ashow' = normalizeRecords . genericAShow'

-- | Turn Rec x [y] ~> (Sub x y)
normalizeRecords :: SExp -> SExp
normalizeRecords = \case
  Rec x [(_,y)] -> Sub [Sym x, normalizeRecords y]
  Rec x xs      -> Rec x $ fmap2 normalizeRecords xs
  Sub xs        -> Sub $ normalizeRecords <$> xs
  Vec xs        -> Vec $ normalizeRecords <$> xs
  Tup xs        -> Tup $ normalizeRecords <$> xs
  x             -> x

-- Fallback ashow functions for queries
sexp :: String -> [SExp] -> SExp
sexp l = Sub . (Sym l:)
recSexp :: String -> [(String, SExp)] -> SExp
recSexp = Rec

data StringVec
data NormalVec
type family VecType a where
  VecType Char = StringVec
  VecType a = NormalVec
class AShowList vt a where
  ashowList' :: Proxy vt -> [a] -> SExp
instance AShow a => AShowList NormalVec a where
  ashowList' _ = Vec . fmap ashow'
instance AShowList StringVec Char where
  ashowList' _ = Str
instance AShowList (VecType a) a => AShow [a] where
  ashow' = ashowList' (Proxy :: Proxy (VecType a))

symShow :: Show a => a -> SExp
symShow = Sym . show
instance AShow SExp where ashow' = id
instance AShow Bool where ashow' = symShow
instance AShow Int where ashow' = symShow
instance AShow Integer where ashow' = symShow
instance AShow Char where ashow' = symShow
instance AShow Double where ashow' = symShow
instance AShow Float where ashow' = symShow
instance AShow Rational where
  ashow' a = Sym $ show (numerator a) ++ "%" ++ show (denominator a)
instance AShow () where ashow' _ = Sym "()"
instance AShow a => AShow (Maybe a)
instance (AShow a, AShow b) => AShow (Either a b)
instance (AShow a, AShow b) => AShow (a,b) where
  ashow' (x,y) = Tup [ashow' x, ashow' y]
instance (AShow a, AShow b, AShow c) => AShow (a,b,c) where
  ashow' (x,y,z) = Tup [ashow' x, ashow' y, ashow' z]
instance (AShow a, AShow b, AShow c, AShow d) => AShow (a,b,c,d) where
  ashow' (x,y,z,w) = Tup [ashow' x, ashow' y, ashow' z, ashow' w]
instance (AShow a, AShow b, AShow c, AShow d, AShow e) => AShow (a,b,c,d,e) where
  ashow' (x,y,z,w,e) = Tup [ashow' x, ashow' y, ashow' z, ashow' w, ashow' e]
instance AShow (f (g a)) => AShow (Compose f g a)
instance AShow a => AShow (Identity a)
instance AShow a => AShow (Const a b)
instance (AShow a, AShow b) => AShow (HM.HashMap a b) where
  ashow' = sexp "HM.fromList" . return . ashow' . HM.toList
instance (AShow a) => AShow (IM.IntMap a) where
  ashow' = sexp "IM.fromList" . return . ashow' . IM.toList
instance AShow IS.IntSet where
  ashow' = sexp "IS.fromList" . return . ashow' . IS.toList
instance (AShowList (VecType a) a, AShow a) => AShow (HS.HashSet a) where
  ashow' = sexp "HS.fromList" . return . ashow' . HS.toList
instance (AShowList (VecType a) a, AShow a) => AShow (DS.Set a) where
  ashow' = sexp "DS.fromList" . return . ashow' . DS.toList
instance AShow a => AShow (Sum a)
instance AShow b => AShow (CoConst a b)
instance AShow a => AShow (Tup2 a)
instance AShowV a => AShow (NEL.NonEmpty a) where
  ashow' (h NEL.:| hs) = ashow' $ h:hs
instance AShow Void where ashow' = undefined
ashowCase' :: (Enum a,Bounded a,Show a,AShow b) => (a -> b) -> SExp
ashowCase' f = Case [(show x,ashow' $ f x) | x <- fullRange]
instance AShow a => AShow (Min a)
