{-# LANGUAGE UndecidableInstances #-}
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

module Control.Antisthenis.AssocContainer
  (AssocContainer(..)
  ,simpleAssocPop
  ,simpleAssocPopNEL
  ,SimpleAssoc(..)) where

import GHC.Generics
import Data.Utils.AShow
import Data.List.NonEmpty as NEL
import Data.Utils.Functors
import Control.Monad.Identity

class Foldable f => AssocContainer f where
  type KeyAC f :: *

  type NonEmptyAC f :: * -> *

  acInsert :: KeyAC f -> a -> f a -> NonEmptyAC f a
  acEmpty :: f a

  -- | A witness that the key is non empty.
  --
  -- acNonEmpty acEmpty == Nothing
  -- isJust $ acNonEmpty $ acInsert k v c
  acNonEmpty :: f a -> Maybe (NonEmptyAC f a)
  acUnlift :: NonEmptyAC f a -> f a


-- | An AssocContainer that has a nonempty version that has zero key
-- at the head if there is one.
data ZeroAssocList f v a =
  ZeroAssocList { malHead :: f (Either a (v,a)),malZ :: [a],malList :: [(v,a)] }

instance Foldable f => Foldable (ZeroAssocList f v) where
  foldr f i mal =
    foldr (f . either id snd) (foldr f (foldr2 f i $ malList mal) $ malZ mal)
    $ malHead mal
  null mal = null $ malHead mal

instance (Eq v,Num v) => AssocContainer (ZeroAssocList Maybe v) where
  type KeyAC (ZeroAssocList Maybe v) = v
  type NonEmptyAC (ZeroAssocList Maybe v) = ZeroAssocList Identity v
  acInsert k a0 mal = case malHead mal of
    Nothing -> ZeroAssocList
      { malHead = insHead,malZ = malZ mal,malList = malList mal }
    Just h@(Left _) -> case runIdentity insHead of
      Left a -> ZeroAssocList
        { malHead = Identity h,malZ = a : malZ mal,malList = malList mal }
      Right a -> ZeroAssocList
        { malHead = Identity h,malZ = malZ mal,malList = a : malList mal }
    Just (Right x) -> ZeroAssocList
      { malHead = insHead,malZ = malZ mal,malList = x : malList mal }
    where
      insHead = Identity $ if k == 0 then Left a0 else Right (k,a0)
  acUnlift mal =
    ZeroAssocList (Just $ runIdentity $ malHead mal) (malZ mal) (malList mal)
  acEmpty = ZeroAssocList Nothing [] []
  acNonEmpty mal =
    (\h -> ZeroAssocList (Identity h) (malZ mal) (malList mal)) <$> malHead mal

newtype SimpleAssoc f k v = SimpleAssoc (f (k,v))
  deriving (Foldable,Functor,Generic)

instance AShow (f (k,v))
  => AShow (SimpleAssoc f k v)

instance AssocContainer (SimpleAssoc [] k) where
  type KeyAC (SimpleAssoc [] k) = k
  type NonEmptyAC (SimpleAssoc [] k) =
    SimpleAssoc (NEL.NonEmpty) k
  acInsert k v (SimpleAssoc m) = SimpleAssoc $ (k,v) NEL.:| m
  acUnlift (SimpleAssoc (m NEL.:| ms)) = SimpleAssoc $ m : ms
  acEmpty = SimpleAssoc []
  acNonEmpty (SimpleAssoc []) = Nothing
  acNonEmpty (SimpleAssoc (x:xs)) = Just $ SimpleAssoc $ x NEL.:| xs

simpleAssocPop :: SimpleAssoc [] k a -> Maybe ((k,a),SimpleAssoc [] k a)
simpleAssocPop (SimpleAssoc []) = Nothing
simpleAssocPop (SimpleAssoc (x:xs)) = Just (x,SimpleAssoc xs)
simpleAssocPopNEL :: SimpleAssoc NEL.NonEmpty k a -> ((k,a),SimpleAssoc [] k a)
simpleAssocPopNEL (SimpleAssoc (x NEL.:| xs)) = (x,SimpleAssoc xs)
