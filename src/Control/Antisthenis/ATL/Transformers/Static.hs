{-# LANGUAGE Arrows                #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Control.Antisthenis.ATL.Transformers.Static
  (StaticArrow(..)) where

import           Control.Arrow
import           Control.Category
import           Control.Antisthenis.ATL.Class.Bind
import           Control.Antisthenis.ATL.Class.Reader
import           Control.Antisthenis.ATL.Class.State
import           Prelude                          hiding (id, (.))

newtype StaticArrow f c x y = StaticArrow { runStaticArrow :: f (c x y) }
instance (Applicative f,Category c) => Category (StaticArrow f c) where
  id = StaticArrow (pure id)
  StaticArrow f . StaticArrow g = StaticArrow $ (.) <$> f <*> g

instance (Applicative f,Arrow c) => Arrow (StaticArrow f c) where
  arr = StaticArrow . pure . arr
  first (StaticArrow f) = StaticArrow $ first <$> f
  second (StaticArrow f) = StaticArrow $ second <$> f
  StaticArrow f *** StaticArrow g = StaticArrow $ (***) <$> f <*> g
  StaticArrow f &&& StaticArrow g = StaticArrow $ (&&&) <$> f <*> g
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (***) #-}
  {-# INLINE (&&&) #-}

instance (Applicative f, ArrowChoice c) => ArrowChoice (StaticArrow f c) where
  left (StaticArrow f) = StaticArrow $ left <$> f
  right (StaticArrow f) = StaticArrow $ right <$> f
  StaticArrow f +++ StaticArrow g = StaticArrow $ (+++) <$> f <*> g
  StaticArrow f ||| StaticArrow g = StaticArrow $ (|||) <$> f <*> g

instance (Applicative f,ArrowState c) => ArrowState (StaticArrow f c) where
  type AStateSt (StaticArrow f c) = AStateSt c
  type AStateArr (StaticArrow f c) =
    StaticArrow f (AStateArr c)
  arrModify (StaticArrow c) = StaticArrow $ arrModify <$> c
  arrCoModify (StaticArrow c) = StaticArrow $ arrCoModify <$> c

instance (Applicative f,ArrowReader c) => ArrowReader (StaticArrow f c) where
  type AReaderR (StaticArrow f c) = AReaderR c
  type AReaderArr (StaticArrow f c) = StaticArrow f (AReaderArr c)
  arrLocal' (StaticArrow c) = StaticArrow $ arrLocal' <$> c
  arrCoLocal' (StaticArrow c) = StaticArrow $ arrCoLocal' <$> c

instance (Applicative f, ArrowBind m c) => ArrowBind m (StaticArrow f c) where
  arrBind (StaticArrow c) (StaticArrow c') = StaticArrow $ arrBind <$> c <*> c'
  arrReturn = StaticArrow $ pure arrReturn
  {-# INLINE arrBind #-}
  {-# INLINE arrReturn #-}
