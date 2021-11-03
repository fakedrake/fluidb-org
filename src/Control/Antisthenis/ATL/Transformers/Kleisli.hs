{-# LANGUAGE Arrows                #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Control.Antisthenis.ATL.Transformers.Kleisli (KleisliArrow(..)) where

import           Control.Antisthenis.ATL.Class.Bind
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Class.Transformer
import           Control.Applicative
import           Control.Arrow
import           Control.Category
import           Data.Functor.Identity
import           Data.Pointed
import           Data.Profunctor
import           Data.Utils.Compose
import           Prelude                                   hiding (id, (.))


newtype KleisliArrow f c x y = KleisliArrow { runKleisliArrow :: c x (f y) }

instance (Applicative f
         ,Arrow (KleisliArrow f c)
         ,Arrow (KleisliArrow f (AStateArr c))
         ,ArrowState c
         ,ArrowBind f c) => ArrowState (KleisliArrow f c) where
  type AStateSt (KleisliArrow f c) = AStateSt c
  type AStateArr (KleisliArrow f c) =
    KleisliArrow f (AStateArr c)
  arrCoModify (KleisliArrow c) =
    KleisliArrow $ arrCoModify c >>> arr (\(s,m) -> (s,) <$> m)
  arrModify (KleisliArrow c) = KleisliArrow $ proc a -> do
    -- retrieve the state change
    bsf :: f (AStateSt c,b) <- arrModify (arr fst &&& c) -< a
    -- register the state change
    arrBindM (arrModify $ arr $ \(_old,(s',b)) -> (s',pure b)) -< bsf

instance (ArrowApply c,Applicative f,ArrowBind f c,Profunctor c)
  => ArrowApply (KleisliArrow f c) where
  app = KleisliArrow $ first (arr runKleisliArrow) >>> app
instance (Applicative f0
         ,ArrowBind f0 c
         ,ArrowBind (Compose f0 m) c
         ,Profunctor c) => ArrowBind m (KleisliArrow f0 c) where
  arrBind (KleisliArrow yz) (KleisliArrow xy) =
    KleisliArrow
    $ arrBind (yz >>> arr Compose) (xy >>> arr Compose) >>> arr getCompose
  arrReturn = KleisliArrow $ arrReturn >>> arr getCompose
  {-# INLINE arrBind #-}
  {-# INLINE arrReturn #-}

instance Applicative f => ArrowTransformer (KleisliArrow f) where
  type TDom (KleisliArrow f) = Identity
  type TCod (KleisliArrow f) = f
  lift' a = KleisliArrow $ arr Identity >>> a
  unlift' (KleisliArrow a) = arr runIdentity >>> a
  {-# INLINE lift' #-}
  {-# INLINE unlift' #-}

instance (Category (KleisliArrow f c),Arrow c,Applicative f,Profunctor c)
  => Arrow (KleisliArrow f c) where
  arr = arrLift . arr
  first (KleisliArrow f) =
    KleisliArrow $ first f >>> arr (\(fc,u) -> (,u) <$> fc)
  {-# INLINE arr #-}
  {-# INLINE first #-}

instance (Category (KleisliArrow f c)
         ,ArrowChoice c
         ,Pointed f
         ,Monad f
         ,Profunctor c) => ArrowChoice (KleisliArrow f c) where
  KleisliArrow l +++ KleisliArrow r =
    KleisliArrow
    $ (l +++ r)
    >>> arr (\case
               Left x  -> Left <$> x
               Right x -> Right <$> x)
instance ArrowBind m c => Category (KleisliArrow m c) where
  KleisliArrow f . KleisliArrow g = KleisliArrow $ f `arrBind` g
  id = KleisliArrow arrReturn
  {-# INLINE (.) #-}
  {-# INLINE id #-}

-- instance (Applicative m,ArrowBind m c,ArrowCont c)
--   => ArrowCont (KleisliArrow m c) where
--   arrCallCC = KleisliArrow $ proc (KleisliArrow c) -> do
--     arrCallCC -< (proc finish -> c -< KleisliArrow $ arrReturn >>> finish)
--   type AContArr (KleisliArrow m c) = KleisliArrow m (AContArr c)
--   liftCC (KleisliArrow c) = KleisliArrow $ liftCC c
