{-# LANGUAGE Arrows                 #-}
{-# LANGUAGE CPP                    #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE UndecidableInstances   #-}
module Control.Antisthenis.ATL.Transformers.Except (ExceptArrow(..)) where

import           Control.Arrow                         hiding ((>>>))
import           Control.Category                      hiding ((>>>))
import           Control.Monad
import           Data.Bifunctor                        (bimap)
import           Data.Functor.Identity
import           Control.Antisthenis.ATL.Class.Cont
import           Control.Antisthenis.ATL.Class.Except
import           Control.Antisthenis.ATL.Class.Machine
import           Control.Antisthenis.ATL.Class.Reader
import           Control.Antisthenis.ATL.Class.Select
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Class.Transformer
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Common
import           Prelude                               hiding (id, (.))

newtype ExceptArrow e c a b = ExceptArrow { runExceptArrow :: c a (Either e b)}
instance ArrowTransformer (ExceptArrow e) where
  type TDom (ExceptArrow e) = Identity
  type TCod (ExceptArrow e) = Either e
  lift' = ExceptArrow . (arr Identity >>>)
  unlift' = (arr runIdentity >>>) . runExceptArrow

instance (ArrowChoice c,ArrowChoice (AWriterArr c),ArrowWriter c)
  => ArrowWriter (ExceptArrow e c) where
  type AWriterW (ExceptArrow e c) = AWriterW c
  type AWriterArr (ExceptArrow e c) =
    ExceptArrow (AWriterW c,e) (AWriterArr c)
  arrListen' (ExceptArrow c) =
    ExceptArrow $ arrListen' c >>> arr (\(w,e) -> bimap (w,) (w,) e)
  arrCoListen' (ExceptArrow c) =
    ExceptArrow
    $ arrCoListen'
    $ c >>> arr (\case
                   Left (w,x) -> (w,Left x)
                   Right (w,x) -> (w,Right x))

instance (ArrowChoice c) => Category (ExceptArrow e c) where
  ExceptArrow f . ExceptArrow g = ExceptArrow $ g >>> arr Left ||| f
  id = ExceptArrow $ arr return . id
  {-# INLINE (.) #-}
  {-# INLINE id #-}

distrib :: (s,Either a b) -> Either (s,a) (s,b)
distrib (s,e) = bimap (s,) (s,) e
{-# INLINE distrib #-}
codistrib :: Either (s,a) (s,b) -> (s,Either a b)
codistrib = \case
  Left (s,x) -> (s,Left x)
  Right (s,x) -> (s,Right x)
{-# INLINE codistrib #-}

-- Failing arrows still modify the state.
instance (ArrowChoice c,ArrowChoice (AStateArr c),ArrowState c)
  => ArrowState (ExceptArrow e c) where
  type AStateSt (ExceptArrow e c) = AStateSt c
  type AStateArr (ExceptArrow e c) =
    ExceptArrow (AStateSt c,e) (AStateArr c)
  arrCoModify (ExceptArrow c) = ExceptArrow $ arrCoModify c >>> arr distrib
  arrModify (ExceptArrow c) = ExceptArrow $ arrModify $ c >>> arr codistrib
  {-# INLINE arrCoModify #-}
  {-# INLINE arrModify #-}

-- Failing arrows still modify the state.
instance (ArrowChoice c,ArrowChoice (AReaderArr c),ArrowReader c)
  => ArrowReader (ExceptArrow e c) where
  type AReaderR (ExceptArrow e c) = AReaderR c
  type AReaderArr (ExceptArrow e c) =
    ExceptArrow e (AReaderArr c)
  arrLocal' (ExceptArrow c) = ExceptArrow $ arrLocal' c
  arrCoLocal' (ExceptArrow c) = ExceptArrow $ arrCoLocal' c

instance (ArrowChoice c,ArrowApply c) => ArrowApply (ExceptArrow e c) where
  app = ExceptArrow $ arr (\(ExceptArrow ab,a) -> (ab,a)) >>> app
  {-# INLINE app #-}

instance ArrowChoice c => Arrow (ExceptArrow f c) where
  arr f = ExceptArrow $ arr $ return . f
  first (ExceptArrow f) =
    ExceptArrow
    $ first f
    >>> arr (\(e,d) -> case e of
               Right x -> Right (x,d)
               Left x  -> Left x)
  second (ExceptArrow f) =
    ExceptArrow
    $ second f
    >>> arr (\(d,e) -> case e of
               Right x -> Right (d,x)
               Left x  -> Left x)
  ExceptArrow f &&& ExceptArrow f' =
    ExceptArrow $ f &&& f' >>> arr (\(a,b) -> (,) <$> a <*> b)
  ExceptArrow f *** ExceptArrow f' =
    ExceptArrow $ f *** f' >>> arr (\(a,b) -> (,) <$> a <*> b)
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (&&&) #-}
  {-# INLINE (***) #-}

instance ArrowChoice c => ArrowChoice (ExceptArrow f c) where
  left (ExceptArrow f) =
    ExceptArrow
    $ left f
    >>> arr (\case
               Left (Left x) -> Left x
               Left (Right x) -> Right $ Left x
               Right x -> Right $ Right x)
  right (ExceptArrow f) =
    ExceptArrow
    $ right f
    >>> arr (\case
               Left x -> Right $ Left x
               Right (Left x) -> Left x
               Right (Right x) -> Right $ Right x)
  ExceptArrow f +++ ExceptArrow g =
    ExceptArrow
    $ f +++ g
    >>> arr
      (\case
         Left (Left x) -> Left x
         Left (Right x) -> Right $ Left x
         Right (Left x) -> Left x
         Right (Right x) -> Right $ Right x)
  ExceptArrow f ||| ExceptArrow g = ExceptArrow $ f ||| g
  {-# INLINE left #-}
  {-# INLINE right #-}
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}

instance ArrowChoice c => ArrowExcept (ExceptArrow e c) where
  type AExceptArr (ExceptArrow e c) = c
  type AExceptE (ExceptArrow e c) = e
  arrUnExcept = runExceptArrow
  arrMkExcept = ExceptArrow
  {-# INLINE arrUnExcept #-}
  {-# INLINE arrMkExcept #-}


class IsEither e x | x -> e where
  type StripEither e x :: *
  unEither :: m (Either e (StripEither e x)) -> m x
  asEither :: m x -> m (Either e (StripEither e x))
instance IsEither e (Either e x) where
  type StripEither e (Either e x) = x
  asEither = id
  {-# INLINE asEither #-}
  unEither = id
  {-# INLINE unEither #-}

instance (ArrowChoice (AContPostArr c)
         ,ArrowChoice (AContFullArr c)
         ,ArrowChoice c
         ,IsEither e (AContC c)
         ,ArrowCont c) => ArrowCont (ExceptArrow e c) where
  type AContFullArr (ExceptArrow e c) =
    ExceptArrow e (AContFullArr c)
  type AContPostArr (ExceptArrow e c) =
    ExceptArrow e (AContPostArr c)
  type AContC (ExceptArrow e c) =
    StripEither e (AContC c)
  liftCC postToFull = ExceptArrow $ liftCC $ \post -> unEither
    $ runExceptArrow
    $ postToFull
    $ ExceptArrow
    $ arr Right >>> asEither post
  dropCC (ExceptArrow c) (ExceptArrow f) =
    ExceptArrow $ asEither $ dropCC c (unEither $ arr Left ||| f)

-- | The errors should affect the telescoped normally.
instance (ArrowMachine c,AMachineNxtArr c ~ c)
  => ArrowMachine (ExceptArrow e c) where
  type AMachineArr (ExceptArrow e c) =
    ExceptArrow e (AMachineArr c)
  type AMachineNxtArr (ExceptArrow e c) =
    ExceptArrow e (AMachineNxtArr c)
  telescope (ExceptArrow c) =
    ExceptArrow $ telescope c >>> arr (\(nxt,ev) -> fmap (ExceptArrow nxt,) ev)
  untelescope (ExceptArrow c) = ExceptArrow ret
    where
      ret =
        untelescope
        $ c >>> arr (\case
                       Left e -> (ret,Left e)
                       Right (ExceptArrow c',e) -> (c',Right e))
  {-# INLINE telescope #-}
  {-# INLINE untelescope #-}

instance (ArrowChoice c
         ,ArrowChoice (ASelArr c)
         ,ArrowChoice (ASelPostArr c)
         ,ArrowSel c
         ,IsEither e (ASelC c)) => ArrowSel (ExceptArrow e c) where
  type ASelArr (ExceptArrow e c) =
    ExceptArrow e (ASelArr c)
  type ASelPostArr (ExceptArrow e c) =
    ExceptArrow e (ASelPostArr c)
  type ASelC (ExceptArrow e c) =
    StripEither e (ASelC c)
  liftSel postToFull = ExceptArrow $ liftSel $ \post -> unEither
    $ runExceptArrow
    $ postToFull
    $ ExceptArrow
    $ arr Right >>> asEither post
  dropSel (ExceptArrow c) (ExceptArrow f) =
    ExceptArrow $ asEither $ dropSel c (unEither $ arr Left ||| f)
  {-# INLINE liftSel #-}
  {-# INLINE dropSel #-}
