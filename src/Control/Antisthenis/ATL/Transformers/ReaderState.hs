{-# LANGUAGE Arrows                #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

-- | Reader and state are interdependent at the ArrowCont instance.
module Control.Antisthenis.ATL.Transformers.ReaderState
  (ReaderArrow(..)
  ,StateArrow(..)
  ,arrDropStateR) where

import           Control.Antisthenis.ATL.Class.Bind
import           Control.Antisthenis.ATL.Class.Cont
import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Class.Machine
import           Control.Antisthenis.ATL.Class.Reader
import           Control.Antisthenis.ATL.Class.Select
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Class.Transformer
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Common
import           Control.Arrow                             hiding ((>>>))
import           Control.Category                          hiding ((>>>))
import           Control.Monad.State
import           Data.Functor.Identity
import           Data.Profunctor
import           Data.Utils.Tup
import           Prelude                                   hiding (id, (.))

newtype ReaderArrow r c x y = ReaderArrow { runReaderArrow :: c (r,x) y }
instance (Profunctor c,ArrowApply c) => ArrowApply (ReaderArrow r c) where
  app = ReaderArrow $ arr (\(~r,(ReaderArrow ab,a)) -> (ab,(r,a))) >>> app
  {-# INLINE app #-}
instance (Profunctor c,Applicative f,ArrowBind f c) => ArrowBind f (ReaderArrow s c) where
  arrBind (ReaderArrow yz) (ReaderArrow xy) = ReaderArrow $
    arrBind yz (arr fst &&& xy >>> arr sequenceA)
  arrReturn = ReaderArrow $ arrReturn >>> arr (fmap snd)
  {-# INLINE arrBind #-}
  {-# INLINE arrReturn #-}

instance (Profunctor c,Arrow c) => ArrowReader (ReaderArrow r c) where
  type AReaderArr (ReaderArrow r c) = c
  type AReaderR (ReaderArrow r c) = r
  arrLocal' = runReaderArrow
  arrCoLocal' = ReaderArrow
  {-# INLINE arrLocal' #-}
  {-# INLINE arrCoLocal' #-}

instance ArrowTransformer (ReaderArrow r) where
  type TDom (ReaderArrow r) = (,) r
  type TCod (ReaderArrow r) = Identity
  lift' x = ReaderArrow $ x >>> arr runIdentity
  unlift' (ReaderArrow x) = x >>> arr Identity
  {-# INLINE lift' #-}
  {-# INLINE unlift' #-}

instance (Profunctor c,Arrow c) => Category (ReaderArrow r c) where
  ReaderArrow bc . ReaderArrow ab =
    ReaderArrow $ arr (\(r,a) -> (r,(r,a))) >>> second ab >>> bc
  id = arrLift id
  {-# INLINE id #-}
  {-# INLINE (.) #-}

instance (Profunctor c,Arrow c) => Arrow (ReaderArrow r c) where
  arr f = ReaderArrow $ arr $ \(_,a) -> f a
  first (ReaderArrow f) =
    ReaderArrow $ arr (\(r,(a,d)) -> ((r,a),d)) >>> first f
  second (ReaderArrow f) =
    ReaderArrow $ arr (\(r,(d,a)) -> (d,(r,a))) >>> second f
  ReaderArrow f &&& ReaderArrow f' = ReaderArrow $ f &&& f'
  ReaderArrow f *** ReaderArrow f' =
    ReaderArrow $ arr (\(r,(a,a')) -> ((r,a),(r,a'))) >>> f *** f'
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (&&&) #-}
  {-# INLINE (***) #-}

instance (Profunctor c,Profunctor (AStateArr c),ArrowState c)
  => ArrowState (ReaderArrow r c) where
  type AStateArr (ReaderArrow r c) =
    ReaderArrow r (AStateArr c)
  type AStateSt (ReaderArrow r c) = AStateSt c
  arrCoModify (ReaderArrow c) =
    ReaderArrow $ arrCoModify c <<< arr (\(r,(s,a)) -> (s,(r,a)))
  arrModify (ReaderArrow c) =
    ReaderArrow $ arrModify $ arr (\(s,(r,a)) -> (r,(s,a))) >>> c
  {-# INLINE arrModify #-}
  {-# INLINE arrCoModify #-}

instance ArrowMachine c => ArrowMachine (StateArrow s c) where
  type AMachineArr (StateArrow s c) =
    StateArrow s (AMachineArr c)
  type AMachineNxtArr (StateArrow s c) =
    StateArrow s (AMachineNxtArr c)
  telescope (StateArrow c) =
    StateArrow $ telescope c >>> arr (\(c',(s,b)) -> (s,(StateArrow c',b)))
  untelescope (StateArrow c) =
    StateArrow $ untelescope $ c >>> arr (\(s,(StateArrow c',b)) -> (c',(s,b)))
  {-# INLINE telescope #-}
  {-# INLINE untelescope #-}

instance (Profunctor c,Profunctor (AMachineArr c),ArrowMachine c)
  => ArrowMachine (ReaderArrow s c) where
  type AMachineArr (ReaderArrow s c) =
    ReaderArrow s (AMachineArr c)
  type AMachineNxtArr (ReaderArrow s c) =
    ReaderArrow s (AMachineNxtArr c)
  telescope (ReaderArrow c) =
    ReaderArrow $ telescope c >>> arr (\(c',b) -> (ReaderArrow c',b))
  untelescope (ReaderArrow c) =
    ReaderArrow $ untelescope $ c >>> arr (\(ReaderArrow c',b) -> (c',b))
  {-# INLINE telescope #-}
  {-# INLINE untelescope #-}

instance (Profunctor c,Profunctor (AWriterArr c),ArrowWriter c)
  => ArrowWriter (ReaderArrow r c) where
  type AWriterArr (ReaderArrow r c) =
    ReaderArrow r (AWriterArr c)
  type AWriterW (ReaderArrow r c) = AWriterW c
  arrCoListen' (ReaderArrow c) = ReaderArrow $ arrCoListen' c
  arrListen' (ReaderArrow c) = ReaderArrow $ arrListen' c

instance (Profunctor c,ArrowChoice c) => ArrowChoice (ReaderArrow r c) where
  left (ReaderArrow x) =
    ReaderArrow
    $ arr (\(r,e) -> case e of
             Left v  -> Left (r,v)
             Right v -> Right v) >>> left x
  right (ReaderArrow x) =
    ReaderArrow
    $ arr (\(r,e) -> case e of
             Right v -> Right (r,v)
             Left v  -> Left v) >>> right x
  ReaderArrow x +++ ReaderArrow y =
    ReaderArrow
    $ arr (\(r,ebb') -> case ebb' of
             Left x0  -> Left (r,x0)
             Right x0 -> Right (r,x0)) >>> x +++ y
  ReaderArrow x ||| ReaderArrow y =
    ReaderArrow
    $ arr (\(r,e) -> case e of
             Right v -> Right (r,v)
             Left v  -> Left (r,v)) >>> x ||| y
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}
  {-# INLINE left #-}
  {-# INLINE right #-}

-- STATE


newtype StateArrow s a b c = StateArrow {runStateArrow :: a (s,b) (s,c)}

instance ArrowApply c => ArrowApply (StateArrow s c) where
  app = StateArrow $ arr (\(s,(StateArrow c,b)) -> (c,(s,b))) >>> app
  {-# INLINE app #-}

instance (ArrowChoice c,ArrowBind Maybe c)
  => ArrowBind Maybe (StateArrow s c) where
  arrBind (StateArrow bc) (StateArrow ab) = StateArrow $ proc (sa,a) -> do
    (sb,mb) <- ab -< (sa,a)
    case mb of
      Just b -> bc -< (sb,b)
      Nothing -> returnA -< (sb,Nothing)
  arrReturn = StateArrow $ second arrReturn
  {-# INLINE arrBind #-}
  {-# INLINE arrReturn #-}

instance Arrow c => ArrowState (StateArrow s c) where
  type AStateSt (StateArrow s c) = s
  type AStateArr (StateArrow s c) = c
  arrCoModify = runStateArrow
  arrModify = StateArrow
  {-# INLINE arrModify #-}
  {-# INLINE arrCoModify #-}

instance Category a => Category (StateArrow s a) where
  id = StateArrow id
  StateArrow f . StateArrow g = StateArrow (f . g)
  {-# INLINE id #-}
  {-# INLINE (.) #-}

instance Arrow a => Arrow (StateArrow s a) where
  arr f = StateArrow $ arr $ second f
  first (StateArrow f) =
    StateArrow
    $ arr (\(s,(b,d)) -> ((s,b),d))
    >>> first f
    >>> arr (\((s,b),d) -> (s,(b,d)))
  second (StateArrow f) =
    StateArrow
    $ arr (\(s,(d,b)) -> (d,(s,b)))
    >>> second f
    >>> arr (\(d,(s,b)) -> (s,(d,b)))
  StateArrow f &&& StateArrow f' =
    StateArrow
    $ arr (\(s,b) -> ((s,b),b))
    >>> first f
    >>> arr (\((s,l),b) -> (l,(s,b)))
    >>> second f'
    >>> arr (\(l,(s,r)) -> (s,(l,r)))
  StateArrow f *** StateArrow f' =
    StateArrow
    $ arr (\(s,(l,r)) -> ((s,l),r))
    >>> first f
    >>> arr (\((s,l),b) -> (l,(s,b)))
    >>> second f'
    >>> arr (\(l,(s,r)) -> (s,(l,r)))
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (&&&) #-}
  {-# INLINE (***) #-}

instance ArrowTransformer (StateArrow s) where
  type TDom (StateArrow s) = (,) s
  type TCod (StateArrow s) = (,) s
  lift' = StateArrow
  unlift' = runStateArrow
  {-# INLINE lift' #-}
  {-# INLINE unlift' #-}

instance ArrowChoice a => ArrowChoice (StateArrow s a) where
  right (StateArrow f) =
    StateArrow
    $ arr (\(s,e) -> case e of
             Right y -> Right (s,y)
             Left y  -> Left (s,y))
    >>> right f
    >>> arr (\e -> case e of
               Right (s,y) -> (s,Right y)
               Left (s,y)  -> (s,Left y))
  left (StateArrow f) =
    StateArrow
    $ arr (\(s,e) -> case e of
             Right y -> Right (s,y)
             Left y  -> Left (s,y))
    >>> left f
    >>> arr (\e -> case e of
               Right (s,y) -> (s,Right y)
               Left (s,y)  -> (s,Left y))
  StateArrow f +++ StateArrow g =
    StateArrow
    $ arr (\(s,e) -> case e of
             Right y -> Right (s,y)
             Left y  -> Left (s,y))
    >>> f +++ g
    >>> arr (\e -> case e of
               Right (s,y) -> (s,Right y)
               Left (s,y)  -> (s,Left y))
  StateArrow f ||| StateArrow g =
    StateArrow
    $ arr (\(s,e) -> case e of
             Right y -> Right (s,y)
             Left y  -> Left (s,y)) >>> f ||| g
  {-# INLINE left #-}
  {-# INLINE right #-}
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}


instance ArrowWriter c => ArrowWriter (StateArrow s c) where
  type AWriterW (StateArrow s c) = AWriterW c
  type AWriterArr (StateArrow s c) =
    StateArrow s (AWriterArr c)
  arrListen' (StateArrow c) =
    StateArrow $ arrListen' c >>> arr (\(w,(s,b)) -> (s,(w,b)))
  arrCoListen' (StateArrow c) =
    StateArrow $ arrCoListen' $ c >>> arr (\(s,(w,b)) -> (w,(s,b)))

-- | Note: State will be used by `exit`.
instance (Profunctor c
         ,Profunctor (AContFullArr c)
         ,Profunctor (AContPostArr c)
         ,ArrowApply c
         ,ArrowCont c) => ArrowCont (StateArrow s c) where
  type AContFullArr (StateArrow s c) =
    ReaderArrow s (AContFullArr c)
  type AContPostArr (StateArrow s c) =
    ReaderArrow s (AContPostArr c)
  type AContC (StateArrow s c) = AContC c
  liftCC c = StateArrow $ liftCC $ runReaderArrow . c . ReaderArrow
  dropCC (StateArrow c) (ReaderArrow post) = ReaderArrow $ dropCC c post

instance (ArrowApply c,ArrowSel c) => ArrowSel (StateArrow s c) where
  type ASelArr (StateArrow s c) =
    StateArrow s (ASelArr c)
  type ASelPostArr (StateArrow s c) =
    ReaderArrow s (ASelPostArr c)
  type ASelC (StateArrow s c) = ASelC c
  liftSel c = StateArrow $ liftSel $ runStateArrow . c . ReaderArrow
  dropSel (StateArrow c) (ReaderArrow post) = StateArrow $ dropSel c post

instance (Profunctor c
         ,ArrowApply c
         ,ArrowCont c
         ,Profunctor (AContFullArr c)
         ,Profunctor (AContPostArr c)) => ArrowCont (ReaderArrow s c) where
  type AContFullArr (ReaderArrow s c) =
    ReaderArrow s (AContFullArr c)
  type AContPostArr (ReaderArrow s c) =
    ReaderArrow s (AContPostArr c)
  type AContC (ReaderArrow s c) = AContC c
  liftCC c =
    ReaderArrow $ liftCC (runReaderArrow . c . ReaderArrow) >>> arr snd
  dropCC (ReaderArrow c) (ReaderArrow post) =
    ReaderArrow $ dropCC (arr fst &&& c) post

arrDropStateR :: ArrowReader c => StateArrow (AReaderR c) c a b -> c a b
arrDropStateR (StateArrow c) = arrCoLocal $ c >>> arr snd

instance ArrowFunctor c => ArrowFunctor (StateArrow s c) where
  type ArrFunctor (StateArrow s c) =
    (StateT s (ArrFunctor c))
  toKleisli (StateArrow c) a = StateT $ \s -> swap <$> toKleisli c (s,a)
  fromKleisli f = StateArrow $ fromKleisli $ \(s,a) -> swap
    <$> runStateT (f a) s
