{-# LANGUAGE Arrows           #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections    #-}
{-# LANGUAGE TypeFamilies     #-}
module Control.Antisthenis.ATL.Class.Select
  (ArrowSel(..)
  ,arrCallSel
  ,arrMapSel
  ,compSel) where

import           Control.Arrow

class (ArrowApply (ASelArr c),Arrow c) => ArrowSel c where
  -- | What is left of the computation
  type ASelArr c :: * -> * -> *
  type ASelPostArr c :: * -> * -> *

  type ASelC c :: *

  liftSel :: (ASelPostArr c b (ASelC c) -> ASelArr c a b) -> c a b
  dropSel :: c a b -> ASelPostArr c b (ASelC c) -> ASelArr c a b


-- Note how these do NOT need postarr and selarr to be arrows
arrCallSel
  :: ArrowSel c => (ASelPostArr c b (ASelC c) -> ASelArr c a b) -> c a b
arrCallSel body = liftSel $ \exit -> body exit

arrMapSel :: ArrowSel c => (ASelArr c a b -> ASelArr c a' b) -> c a b -> c a' b
arrMapSel f = liftSel . fmap f . dropSel

-- Consider the type

-- test :: ArrowApply c
--      => (c b r -> ReaderArrow r c x b)
--      -> (c x r -> ReaderArrow r c a x)
--      -> c b r -> ReaderArrow r c a b
-- test = compSel $ \mk (ReaderArrow next) post -> ReaderArrow
--   $ proc (r,x)
--   -> app -< (runReaderArrow $ mk $ arr (r,) >>> next >>> post,(r,x))

compSel
  :: (Arrow c0,Arrow c)
  => ((c0 x r -> c a x) -> c x b -> c0 b r -> c a x)
  -> (c0 b r -> c x b)
  -> (c0 x r -> c a x)
  -> (c0 b r -> c a b)
compSel f l r br0 =
  let xb = l br0
  in (f r xb br0) >>> xb
