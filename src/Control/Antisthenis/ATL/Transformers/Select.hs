{-# LANGUAGE Arrows               #-}
{-# LANGUAGE CPP                  #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TupleSections        #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
module Control.Antisthenis.ATL.Transformers.Select (SelectArrow(..)) where

import           Control.Arrow                    hiding ((>>>))
import qualified Control.Category                 as C
import           Control.Monad
import           Control.Antisthenis.ATL.Class.Except
import           Control.Antisthenis.ATL.Class.Reader
import           Control.Antisthenis.ATL.Class.Select
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Common

newtype SelectArrow r c a b =
  SelectArrow
  { runSelectArrow :: c b r -> c a b
  }

instance C.Category c => C.Category (SelectArrow r c) where
  SelectArrow bc . SelectArrow ab = SelectArrow go where
    go cr = ab (bc' >>> cr) >>> bc' where
      bc' = bc cr
  id = SelectArrow $ const C.id
  {-# INLINE id #-}
  {-# INLINE (.) #-}

instance ArrowApply c => Arrow (SelectArrow r c) where
  arr ab = SelectArrow $ const $ arr ab
  first (SelectArrow bc) = SelectArrow $ \cdr -> arr
    (\(b,d) -> ((bc (arr (\c -> (c,d)) >>> cdr),b),d))
    >>> first app
  second (SelectArrow bc) = SelectArrow $ \dcr -> arr
    (\(d,b) -> (d,(bc (arr (\c -> (d,c)) >>> dcr),b)))
    >>> second app
  SelectArrow bc *** SelectArrow bc' = SelectArrow go
    where
      go exit =
        arr (\(b,b') -> ((bc $ arr (\c -> (rest c,b')) >>> app >>> exit,b),b'))
        >>> first app
        >>> arr (\(c,b') -> (rest c,b'))
        >>> app
        where
          rest c = bc' (arr (c,) >>> exit) >>> arr (c,)
  SelectArrow bc &&& SelectArrow bc' = SelectArrow go
    where
      go exit =
        arr (\b -> ((bc $ arr (\c -> (rest c,b)) >>> app >>> exit,b),b))
        >>> first app
        >>> arr (\(c,b) -> (rest c,b))
        >>> app
        where
          rest c = bc' (arr (c,) >>> exit) >>> arr (c,)
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (***) #-}
  {-# INLINE (&&&) #-}

instance (ArrowChoice c,ArrowApply c) => ArrowChoice (SelectArrow r c) where
  SelectArrow x +++ SelectArrow y = SelectArrow $ \ecr
    -> x (arr Left >>> ecr) +++ y (arr Right >>> ecr)
  SelectArrow x ||| SelectArrow y = SelectArrow $ \ecr -> x ecr ||| y ecr
  left (SelectArrow y) = SelectArrow $ \ecr -> left $ y $ arr Left >>> ecr
  right (SelectArrow y) = SelectArrow $ \ecr -> right $ y $ arr Right >>> ecr
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}
  {-# INLINE left #-}
  {-# INLINE right #-}

instance (ArrowExcept c
         ,ArrowApply c
         ,ArrowApply (AExceptArr c)
         ,ArrowChoice (AExceptArr c)
         ,e ~ AExceptE c) => ArrowExcept (SelectArrow (Either e r) c) where
  type AExceptArr (SelectArrow (Either e r) c) =
    SelectArrow (Either e r) (AExceptArr c)
  type AExceptE (SelectArrow (Either e r) c) = AExceptE c
  arrUnExcept (SelectArrow c) = SelectArrow $ \ayr
    -> arrUnExcept $ c $ arrMkExcept $ proc y -> ayr >>> arr Right -< (Right y)
  arrMkExcept (SelectArrow c) = SelectArrow $ \yr
    -> arrMkExcept $ c $ arr Left ||| (arrUnExcept yr >>> arr join)
  {-# INLINE arrUnExcept #-}
  {-# INLINE arrMkExcept #-}

instance ArrowApply c => ArrowApply (SelectArrow r c) where
  app = SelectArrow $ \br -> arr (\(SelectArrow ab,a) -> (ab br,a)) >>> app
  {-# INLINE app #-}

instance (ArrowApply c,ArrowApply (AStateArr c),ArrowState c)
  => ArrowState (SelectArrow r c) where
  type AStateSt (SelectArrow r c) = AStateSt c
  type AStateArr (SelectArrow r c) =
    SelectArrow (AStateSt c,r) (AStateArr c)
  arrModify (SelectArrow c) = SelectArrow $ \f -> arrModify $ c $ arrCoModify f
  arrCoModify (SelectArrow c) =
    SelectArrow $ \f -> arrCoModify $ c $ arrModify f
  {-# INLINE arrModify #-}
  {-# INLINE arrCoModify #-}

instance ArrowApply c => ArrowSel (SelectArrow r c) where
  type ASelArr (SelectArrow r c) = c
  type ASelPostArr (SelectArrow r c) = c
  type ASelC (SelectArrow r c) = r
  liftSel = SelectArrow
  dropSel = runSelectArrow
  {-# INLINE liftSel #-}
  {-# INLINE dropSel #-}

instance (ArrowApply (AReaderArr c),ArrowApply c,ArrowReader c)
  => ArrowReader (SelectArrow r c) where
  type AReaderArr (SelectArrow r c) =
    SelectArrow r (AReaderArr c)
  type AReaderR (SelectArrow r c) = AReaderR c
  arrLocal' (SelectArrow c) =
    SelectArrow $ \exit -> arrLocal' $ c $ arrCoLocal' $ arr snd >>> exit
  arrCoLocal' (SelectArrow c) =
    SelectArrow $ \exit -> arrCoLocal' $ proc (r,a) -> do
    app -< (c $ arr (r,) >>> arrLocal' exit,(r,a))
  {-# INLINE arrLocal' #-}
  {-# INLINE arrCoLocal' #-}

instance (ArrowChoice (AWriterArr c)
         ,ArrowApply (AWriterArr c)
         ,ArrowApply c
         ,ArrowWriter c
         ,Monoid (AWriterW c)
         ,ArrowChoice c) => ArrowWriter (SelectArrow r c) where
  type AWriterArr (SelectArrow r c) =
    SelectArrow r (AWriterArr c)
  type AWriterW (SelectArrow r c) = AWriterW c
  arrListen' (SelectArrow c) = SelectArrow $ \post
    -> arrListen' $ c $ arrCoListen' $ arr (mempty,) >>> (arr fst &&& post)
  arrCoListen' (SelectArrow c) = SelectArrow $ \post
    -> arrCoListen' $ c $ arr snd >>> arrListen' post >>> arr snd
