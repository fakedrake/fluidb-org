{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE Arrows                #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Control.Antisthenis.ATL.Transformers.Writer
  (WriterArrow(..)) where

import Data.Utils.Tup
import Control.Monad.Writer
import Control.Antisthenis.ATL.Class.Functorial
import Data.Profunctor
import           Control.Arrow
import           Control.Category
import           Data.Functor.Identity
import           Control.Antisthenis.ATL.Class.Bind
import           Control.Antisthenis.ATL.Class.Machine
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Class.Transformer
import           Control.Antisthenis.ATL.Class.Writer
import           Prelude                                hiding (id, (.))

newtype WriterArrow w c x y = WriterArrow { runWriterArrow :: c x (w,y) }
instance (Arrow c,Monoid w) => ArrowWriter (WriterArrow w c) where
  type AWriterW (WriterArrow w c) = w
  type AWriterArr (WriterArrow w c) = c
  arrListen' (WriterArrow c) = c
  arrCoListen' = WriterArrow
instance (Monoid w,ArrowApply c) => ArrowApply (WriterArrow w c) where
  app = WriterArrow $ proc (WriterArrow ab,a) -> app -< (ab,a)
  {-# INLINE app #-}
instance (ArrowBind Maybe c,ArrowChoice c,Monoid s)
  => ArrowBind Maybe (WriterArrow s c) where
  arrBind (WriterArrow bc) (WriterArrow ab) = WriterArrow $ proc a -> do
    (sb,mb) <- ab -< a
    case mb of
      Nothing -> returnA -< (sb,Nothing)
      Just b -> do
        (sc,c) <- bc -< b
        returnA -< (sc <> sb,c)
  arrReturn = WriterArrow $ arrReturn >>> arr (mempty,)
  {-# INLINE arrBind #-}
  {-# INLINE arrReturn #-}

instance ArrowTransformer (WriterArrow w) where
  type TDom (WriterArrow w) = Identity
  type TCod (WriterArrow w) = (,) w
  lift' x = WriterArrow $ arr Identity >>> x
  unlift' (WriterArrow x) = arr runIdentity >>> x
  {-# INLINE unlift' #-}
  {-# INLINE lift' #-}

instance (Monoid w,Arrow c) => Category (WriterArrow w c) where
  WriterArrow bc . WriterArrow ab =
    WriterArrow $ ab >>> id *** bc >>> arr (\(s,(s',x)) -> (s <> s',x))
  id = arrLift id
  {-# INLINE (.) #-}
  {-# INLINE id #-}

instance (Monoid w,Arrow c) => Arrow (WriterArrow w c) where
  arr f = arrLift (arr f)
  first (WriterArrow f) = WriterArrow $ first f >>> arr (\((w,c),d) -> (w, (c,d)))
  {-# INLINE arr #-}
  {-# INLINE first #-}

instance (Monoid r,ArrowState c) => ArrowState (WriterArrow r c) where
  type AStateSt (WriterArrow r c) = AStateSt c
  type AStateArr (WriterArrow r c) =
    (WriterArrow r (AStateArr c))
  arrCoModify (WriterArrow c) =
    WriterArrow $ arrCoModify c >>> arr (\(s,(r,b)) -> (r,(s,b)))
  arrModify (WriterArrow c) =
    WriterArrow $ arrModify $ c >>> arr (\(s,(r,a)) -> (r,(s,a)))

instance (Monoid w,ArrowChoice c) => ArrowChoice (WriterArrow w c) where
  WriterArrow l +++ WriterArrow r =
    WriterArrow
    $ l +++ r
    >>> arr (\case
               Left (w,x) -> (w,Left x)
               Right (w,x) -> (w,Right x))
  {-# INLINE (+++) #-}

instance (Monoid w,ArrowMachine c) => ArrowMachine (WriterArrow w c) where
  type AMachineArr (WriterArrow w c) =
    WriterArrow w (AMachineArr c)
  type AMachineNxtArr (WriterArrow w c) =
    WriterArrow w (AMachineNxtArr c)
  telescope (WriterArrow c) = WriterArrow $ proc a -> do
    (nxt,(w,v)) <- telescope c -< a
    returnA -< (w,(WriterArrow nxt,v))
  untelescope (WriterArrow c) = WriterArrow $ untelescope $ proc a -> do
    (w,(WriterArrow nxt,b)) <- c -< a
    returnA -< (nxt,(w,b))

instance Profunctor c => Profunctor (WriterArrow w c) where
  dimap f g (WriterArrow c) =
    WriterArrow $ dimap f (second g) c
  {-# INLINE dimap #-}

instance ArrowFunctor c => ArrowFunctor (WriterArrow w c) where
  type ArrFunctor (WriterArrow w c) =
    WriterT w (ArrFunctor c)
  toKleisli (WriterArrow c) = WriterT . fmap swap . toKleisli c
  fromKleisli c = WriterArrow $ fromKleisli $ fmap (fmap swap . runWriterT) c
