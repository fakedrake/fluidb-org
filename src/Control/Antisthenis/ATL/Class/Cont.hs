{-# LANGUAGE Arrows                #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE TypeFamilies          #-}
module Control.Antisthenis.ATL.Class.Cont (ArrowCont(..),arrCallCC) where

import           Control.Arrow

class (ArrowApply (AContPostArr c),ArrowApply (AContFullArr c),Arrow c)
  => ArrowCont (c :: * -> * -> *) where
  -- | What is left of the computation
  type AContPostArr c :: * -> * -> *
  -- | The entire computation
  type AContFullArr c :: * -> * -> *

  type AContC c :: *

  liftCC :: (AContPostArr c b (AContC c) -> AContFullArr c a (AContC c))
         -> c a b
  dropCC :: c a b -> AContPostArr c b (AContC c) -> AContFullArr c a (AContC c)

arrCallCC
  :: ArrowCont c
  => (AContPostArr c a (AContC c) -> AContFullArr c a (AContC c))
  -> c (AContFullArr c (AContPostArr c a (AContC c)) a) a
arrCallCC f = liftCC $ \exit -> proc body -> app -< (body >>> f exit,exit)
