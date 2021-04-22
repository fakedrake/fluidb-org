{-# LANGUAGE Arrows               #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE LambdaCase           #-}
{-# LANGUAGE TupleSections        #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
module Control.Antisthenis.ATL.Transformers.SMArrow
  (SMArrow(..)
  ,liftSM
  ,untelescopeSM) where

import           Control.Arrow                           hiding ((>>>))
import           Control.Category                        hiding ((>>>))
import           Control.Antisthenis.ATL.Class.Machine
import           Control.Antisthenis.ATL.Class.Reader
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Class.Transformer
import           Control.Antisthenis.ATL.Common
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Select
import           Prelude                                 hiding (id, (.))

-- | Note that for continuation we need the next. Programs are not
-- DAGs, they are graphs.
newtype SMArrow r c a b =
  SMArrow { runSMArrow :: c (SMArrow r c a b,b) r -> c a (SMArrow r c a b,b)
          }

instance (ArrowApply c,ArrowApply (AReaderArr c),ArrowReader c)
  => ArrowReader (SMArrow r c) where
  type AReaderArr (SMArrow r c) =
    SMArrow r (AReaderArr c)
  type AReaderR (SMArrow r c) = AReaderR c
  arrCoLocal' (SMArrow c) =
    SMArrow
    $ runSelectArrow
    $ arrCoLocal'
    $ SelectArrow c >>> arr (\(nxt,b) -> (arrCoLocal' nxt,b))
  arrLocal' (SMArrow c) =
    SMArrow
    $ runSelectArrow
    $ arrLocal'
    $ SelectArrow c >>> arr (\(nxt,b) -> (arrLocal' nxt,b))
  {-# INLINE arrLocal' #-}
  {-# INLINE arrCoLocal' #-}

instance (ArrowApply c,ArrowApply (AStateArr c),ArrowState c)
  => ArrowState (SMArrow r c) where
  type AStateArr (SMArrow r c) =
    SMArrow (AStateSt c,r) (AStateArr c)
  type AStateSt (SMArrow r c) = AStateSt c
  arrModify (SMArrow c) =
    SMArrow
    $ runSelectArrow
    $ arrModify
    $ SelectArrow c >>> arr (\(nxt,(s',b)) -> (s',(arrModify nxt,b)))
  arrCoModify (SMArrow c) =
    SMArrow
    $ runSelectArrow
    $ (arrCoModify $ SelectArrow c)
    >>> arr (\(s,(nxt,b)) -> (arrCoModify nxt,(s,b)))
  {-# INLINE arrModify #-}
  {-# INLINE arrCoModify #-}

instance ArrowApply c => Category (SMArrow r c) where
  id = SMArrow $ const $ arr $ (id,)
  SMArrow bc . SMArrow ab = SMArrow $ go
    where
      go exit = ab (postAB >>> exit) >>> postAB
        where
          postAB =
            arr
              (\(nxtAB,b) -> ((bc $ arr (first (nxtAB >>>)) >>> exit,b),nxtAB))
            >>> first app
            >>> arr (\((nxtBC,c),nxtAB) -> (nxtAB >>> nxtBC,c))
  {-# INLINE id #-}
  {-# INLINE (.) #-}

instance ArrowApply c => Arrow (SMArrow r c) where
  arr f = SMArrow $ const $ arr $ \a -> (arr f,f a)
  first (SMArrow f) = SMArrow $ \exit -> arr
    (\(a,d) -> ((f $ arr (\(nxt,b) -> (first nxt,(b,d))) >>> exit,a),d))
    >>> first app
    >>> arr (\((nxt,b),d) -> (first nxt,(b,d)))
  -- first (SMArrow s) =
  --   SMArrow
  --   $ runSelectArrow
  --   $ first (SelectArrow s)
  --   >>> arr (\((nxt,c),d) -> (first nxt,(c,d)))
  second (SMArrow s) =
    SMArrow
    $ runSelectArrow
    $ second (SelectArrow s)
    >>> arr (\(d,(nxt,c)) -> (second nxt,(d,c)))
  SMArrow s *** SMArrow s' =
    SMArrow
    $ runSelectArrow
    $ (SelectArrow s *** SelectArrow s')
    >>> arr (\((nxtl,l),(nxtr,r)) -> (nxtl *** nxtr,(l,r)))
  SMArrow s &&& SMArrow s' =
    SMArrow
    $ runSelectArrow
    $ (SelectArrow s &&& SelectArrow s')
    >>> arr (\((nxtl,l),(nxtr,r)) -> (nxtl &&& nxtr,(l,r)))
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (***) #-}

instance (ArrowChoice c,ArrowApply c) => ArrowMachine (SMArrow r c) where
  type AMachineArr (SMArrow r c) = SelectArrow r c
  type AMachineNxtArr (SMArrow r c) = SMArrow r c
  telescope (SMArrow c) = SelectArrow c
  untelescope (SelectArrow c) = SMArrow c
  {-# INLINE telescope #-}
  {-# INLINE untelescope #-}

-- Note that we can't get a ArrowSel instance.
instance (ArrowChoice c,ArrowApply c) => ArrowChoice (SMArrow r c) where
  right (SMArrow f) = SMArrow $ \exit
    -> right (f $ arr post0 >>> exit) >>> post
    where
      post0 (f',x) = (right f',Right x)
      post = arr (\case
                    Left e -> (right (SMArrow f),Left e)
                    Right (f',x) -> (right f',Right x))
  left (SMArrow f) = SMArrow $ \exit
    -> left (f $ arr post0 >>> exit) >>> post
    where
      post0 (f',x) = (left f',Left x)
      post = arr (\case
                    Right e -> (left (SMArrow f),Right e)
                    Left (f',x) -> (left f',Left x))
  SMArrow ad +++ SMArrow bd =
    SMArrow $ \exit -> (ad (arr ((+++ SMArrow bd) *** Left) >>> exit))
    +++ (bd (arr ((SMArrow ad +++) *** Right) >>> exit))
    >>> arr (\case
               Left (x,c) -> (x +++ SMArrow bd,Left c)
               Right (x,c) -> (SMArrow ad +++ x,Right c))
  SMArrow ad ||| SMArrow bd =
    SMArrow $ \exit -> (ad (arr (first (||| SMArrow bd)) >>> exit))
    +++ (bd (arr (first (SMArrow ad |||)) >>> exit))
    >>> arr (\case
               Left (x,d) -> (x ||| SMArrow bd,d)
               Right (x,d) -> (SMArrow ad ||| x,d))
  {-# INLINE right #-}
  {-# INLINE left #-}
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}

-- Drop the given arrow's next generation and replace and replace it
-- with the output.
untelescopeSM
  :: ArrowApply c => SMArrow r c a (SMArrow r c a b,b) -> SMArrow r c a b
untelescopeSM (SMArrow c) = SMArrow $ \exit -> c (arr snd >>> exit) >>> arr snd
{-# INLINE untelescopeSM #-}

liftSM :: Arrow c => MealyArrow c a b -> SMArrow r c a b
liftSM (MealyArrow c) = SMArrow $ const $ c >>> arr (first liftSM)
