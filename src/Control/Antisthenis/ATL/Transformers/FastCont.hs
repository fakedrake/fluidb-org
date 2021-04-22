{-# LANGUAGE Arrows                #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# LANGUAGE ViewPatterns          #-}
module Control.Antisthenis.ATL.Transformers.FastCont
  (ContArrow(..)) where

import           Control.Arrow
import           Control.Category
import           Control.Antisthenis.ATL.Class.Cont
import           Control.Antisthenis.ATL.Class.State
import           Prelude                                  hiding (id, (.))


-- | Note: The ordering around ContArrow has the meaning that the
-- effects of the outer layers are all stripped when computing r. The
-- effects of the underneath layers are kept.
newtype ContArrow r c a b = ContArrow { runContArrow :: c b r -> c a r }

arrCurry :: Arrow c => c (x,y) z -> c x (c y z)
arrCurry xyz = proc x -> returnA -< proc y -> xyz -< (x,y)
{-# INLINE arrCurry #-}
appSecond :: Arrow c => c (x,y) z -> c y (c x z)
appSecond xyz = proc y -> returnA -< proc x -> xyz -< (x,y)
{-# INLINE appSecond #-}

instance ArrowApply c => Category (ContArrow r c) where
  id = ContArrow id
  ContArrow crbr . ContArrow brar = ContArrow $ brar . crbr
  {-# INLINE id #-}
  {-# INLINE (.) #-}

instance ArrowApply c => Arrow (ContArrow r c) where
  arr bc = ContArrow go where go cr = proc b -> cr -< bc b
  first (ContArrow crbr) = ContArrow go where
    go cdr =  proc (b,d) -> do
      cr <- appSecond cdr -< d
      app -< (crbr cr,b)
  second (ContArrow crbr) = ContArrow $ \cdr -> proc (d,b) -> do
    cr <- arrCurry cdr -< d
    app -< (crbr cr,b)
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}

instance ArrowApply c => ArrowCont (ContArrow r c) where
  type AContC (ContArrow r c) = r
  type AContFullArr (ContArrow r c) = c
  type AContPostArr (ContArrow r c) = c
  liftCC = ContArrow
  dropCC = runContArrow

instance ArrowApply c => ArrowApply (ContArrow r c) where
  app = ContArrow $ \cr -> proc (ContArrow crbr,b) -> app -< (crbr cr,b)
  {-# INLINE app #-}

arrCallCC'
  :: ArrowApply c => ContArrow r c (c (c a r) a) a
arrCallCC' = ContArrow $ \exit -> proc body -> app -< (body >>> exit,exit)

arrCallCC0
  :: ArrowApply c => ContArrow r c (ContArrow r c (ContArrow r c a r) a) a
arrCallCC0 = ContArrow $ \exit
  -> proc (ContArrow body) -> app -< (body exit,ContArrow $ \_ -> exit)

-- NO WRITER INSTANCE DUE TO arrListen

instance (ArrowApply c,ArrowChoice c) => ArrowChoice (ContArrow r c) where
  left (ContArrow c) = ContArrow $ \cdr -> c (arr Left >>> cdr)
    ||| (arr Right >>> cdr)

instance (ArrowApply c,ArrowApply (AStateArr c),ArrowState c)
  => ArrowState (ContArrow r c) where
  type AStateSt (ContArrow r c) = AStateSt c
  type AStateArr (ContArrow r c) =
    ContArrow (AStateSt c,r) (AStateArr c)
  arrModify (ContArrow c) = ContArrow $ \f -> arrModify $ c $ arrCoModify f
  arrCoModify (ContArrow c) = ContArrow $ \f -> arrCoModify $ c $ arrModify f
