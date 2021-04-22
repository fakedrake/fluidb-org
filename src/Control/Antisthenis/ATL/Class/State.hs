{-# LANGUAGE Arrows                 #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}

module Control.Antisthenis.ATL.Class.State
  (ArrowState(..)
  ,Focus
  ,arrModify2
  ,arrGet
  ,unfocusArr) where

import           Control.Arrow

class (Arrow c,Arrow (AStateArr c)) => ArrowState c where
  type AStateSt c :: *

  type AStateArr c :: * -> * -> *

  arrModify :: AStateArr c (AStateSt c,a) (AStateSt c,b) -> c a b
  arrCoModify :: c a b -> AStateArr c (AStateSt c,a) (AStateSt c,b)

arrGet :: ArrowState c => c () (AStateSt c)
arrGet = arrModify $ arr $ \(s,()) -> (s,s)

arrModify2
  :: (ArrowState (AStateArr c),ArrowState c)
  => AStateArr
    (AStateArr c)
    (AStateSt (AStateArr c),a)
    (AStateSt (AStateArr c),b)
  -> c a b
arrModify2 c = arrModify $ arrModify $ proc (st0,(st1,a)) -> do
  (st0',b) <- c -< (st0,a)
  returnA -< (st0',(st1,b))

focus :: Arrow c => (a -> (b' -> b, a')) -> (c a' b') -> (c a b)
focus f ab' = proc a -> do
  (grow,a') <- arr f -< a
  b' <- ab' -< a'
  returnA -< grow b'

dropAB :: (s' -> (s -> s', s)) -> ((s', a) -> ((s, b) -> (s', b), (s, a)))
dropAB cs (s',a) =
  let (ss',s) = cs s'
  in (first ss', (s, a))

-- | The way to adapt a computation in a small structure to a larger
-- one. Like lenses but more specific. Eg
--
-- onSecond :: (b -> b) -> Focus (->) b (a,b,c)
-- onSecond f (a,b,c) = (\b' -> (a,b',c),f b)
--
-- Replace the second element
-- onSndList :: (a -> a) -> Focus (->) a [a]
-- onSndList f (x:y:xs) = (\y' -> x:y:xs,f y)

type Focus c small large = (c large (c small large, small))
-- | Expand the focused state of arrow to a larger structure.
unfocusArr
  :: (ArrowState c,ArrowState c',AStateArr c ~ AStateArr c')
  => Focus (->) (AStateSt c) (AStateSt c')
  -> c a b
  -> c' a b
unfocusArr x = arrModify . focus (dropAB x) . arrCoModify
