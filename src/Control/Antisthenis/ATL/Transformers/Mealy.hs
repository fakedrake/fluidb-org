{-# LANGUAGE Arrows                #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}
module Control.Antisthenis.ATL.Transformers.Mealy
  (MealyArrow(..)
  ,MB
  ,squashMealy
  ,hoistMealy
  ,mkMealy
  ,mealyScan
  ,mealyLift
  ,yieldMB
  ,finishMB) where

import           Control.Antisthenis.ATL.Class.Bind
import           Control.Antisthenis.ATL.Class.Functorial
import           Control.Antisthenis.ATL.Class.Machine
import           Control.Antisthenis.ATL.Class.Reader
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Class.Writer
import           Control.Antisthenis.ATL.Common
import           Control.Arrow                            hiding ((>>>))
import           Control.Category                         hiding ((>>>))
import           Control.Monad.Fix
import           Control.Utils.Free
import           Data.Bifunctor                           (bimap)
import           Data.Maybe
import           Data.Profunctor
import           Data.Void
import           Prelude                                  hiding (id, (.))

-- | Note that this is not a transformer because it is not isomorphic
-- to c.
newtype MealyArrow c a b = MealyArrow { runMealyArrow :: c a (MealyArrow c a b,b) }


-- NOTE: MACHINES DO NOT SUPPORT APPLY: Apply is unchanged to the next
-- generations. That means that the next generations of the argument
-- must be dropped.
--
-- instance ArrowApply c => ArrowApply (MealyArrow c) where
--   app = MealyArrow $ proc (MealyArrow ab,a) -> do
--     (ab',b) <- app -< (ab,a)
--     -- we are dropping the
--     returnA -< (arr _snd >>> ab',b)
--   {-# INLINE app #-}

instance ArrowState c => ArrowState (MealyArrow c) where
  type AStateSt (MealyArrow c) = AStateSt c
  type AStateArr (MealyArrow c) =
    MealyArrow (AStateArr c)
  arrCoModify (MealyArrow c) =
    MealyArrow $ arrCoModify c >>> arr (\(s,(c',b)) -> (arrCoModify c',(s,b)))
  arrModify (MealyArrow c) =
    MealyArrow $ arrModify $ c >>> arr (\(c',(s,b)) -> (s,(arrModify c',b)))
  {-# INLINE arrCoModify #-}
  {-# INLINE arrModify #-}

instance ArrowBind Maybe c => ArrowBind Maybe (MealyArrow c) where
  arrBind (MealyArrow yz) (MealyArrow xy) = MealyArrow $ proc x -> do
    (xy',my) <- xy -< x
    e <- arrBindM (yz >>> arr Just) -< my
    (yz',mz) <- arr (fromMaybe (MealyArrow yz,Nothing)) -< e
    returnA -< (arrBind yz' xy', mz)
  arrReturn = MealyArrow $ id >>> arr ((arrReturn,) . Just)
  {-# INLINE arrBind #-}
  {-# INLINE arrReturn #-}

instance Arrow c => Category (MealyArrow c) where
  MealyArrow bc . MealyArrow ab =
    MealyArrow $ ab >>> second bc >>> arr (\(ab',(bc',c)) -> (ab' >>> bc',c))
  id = MealyArrow $ arr (id,)
  {-# INLINE id #-}
  {-# INLINE (.) #-}

instance Arrow c => Arrow (MealyArrow c) where
  arr f = ret
    where
      ret = MealyArrow $ arr $ (ret,) . f
  first (MealyArrow bc) =
    MealyArrow $ first bc >>> arr (\((nxt,c),d) -> (first nxt,(c,d)))
  second (MealyArrow bc) =
    MealyArrow $ second bc >>> arr (\(d,(nxt,c)) -> (second nxt,(d,c)))
  MealyArrow bc &&& MealyArrow bc' =
    MealyArrow $ bc &&& bc' >>> arr (\((n,c),(n',c')) -> (n &&& n',(c,c')))
  MealyArrow bc *** MealyArrow bc' =
    MealyArrow $ bc *** bc' >>> arr (\((n,c),(n',c')) -> (n *** n',(c,c')))
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (&&&) #-}

instance ArrowChoice c => ArrowChoice (MealyArrow c) where
  MealyArrow f +++ MealyArrow g = MealyArrow $ proc a -> case a of
    Left l -> do
      (nxt,b) <- f -< l
      returnA -< (nxt +++ MealyArrow g,Left b)
    Right r -> do
      (nxt,b) <- g -< r
      returnA -< (MealyArrow f +++ nxt,Right b)
  MealyArrow f ||| MealyArrow g = MealyArrow $ proc a -> case a of
    Left l -> do
      (nxt,b) <- f -< l
      returnA -< (nxt ||| MealyArrow g,b)
    Right r -> do
      (nxt,b) <- g -< r
      returnA -< (MealyArrow f ||| nxt,b)
  left (MealyArrow f) = MealyArrow $ proc a -> case a of
    Left l -> do
      (nxt,b) <- f -< l
      returnA -< (left nxt,Left b)
    Right r -> returnA -< (left $ MealyArrow f,Right r)
  right (MealyArrow f) = MealyArrow $ proc a -> case a of
    Right r -> do
      (nxt,b) <- f -< r
      returnA -< (right nxt,Right b)
    Left l -> returnA -< (right $ MealyArrow f,Left l)
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}
  {-# INLINE left #-}
  {-# INLINE right #-}

instance ArrowChoice c => ArrowMachine (MealyArrow c) where
  type AMachineArr (MealyArrow c) = c
  type AMachineNxtArr (MealyArrow c) = MealyArrow c
  telescope = runMealyArrow
  untelescope = MealyArrow
  {-# INLINE telescope #-}
  {-# INLINE untelescope #-}

instance ArrowWriter c => ArrowWriter (MealyArrow c) where
  type AWriterArr (MealyArrow c) =
    MealyArrow (AWriterArr c)
  type AWriterW (MealyArrow c) = AWriterW c
  arrListen' (MealyArrow c) =
    MealyArrow $ arrListen' c >>> arr (\(w,(nxt,b)) -> (arrListen' nxt,(w,b)))
  arrCoListen' (MealyArrow c) =
    MealyArrow
    $ arrCoListen'
    $ c >>> arr (\(nxt,(w,b)) -> (w,(arrCoListen' nxt,b)))

instance ArrowReader c  => ArrowReader (MealyArrow c) where
  type AReaderArr (MealyArrow c) = MealyArrow (AReaderArr c)
  type AReaderR (MealyArrow c) = AReaderR c
  arrLocal' = MealyArrow . (>>> first (arr arrLocal')) . arrLocal' . runMealyArrow
  arrCoLocal' = MealyArrow . (>>> first (arr arrCoLocal')) . arrCoLocal'  . runMealyArrow
  {-# INLINE arrLocal' #-}
  {-# INLINE arrCoLocal' #-}



hoistMealy
  :: Profunctor c'
  => (c a (MealyArrow c a b,b) -> c' a' (MealyArrow c a b,b'))
  -> MealyArrow c a b
  -> MealyArrow c' a' b'
hoistMealy f = go
  where
    go (MealyArrow c) = MealyArrow $ rmap (first go) $ f c

instance Profunctor c => Profunctor (MealyArrow c) where
  rmap f = go
    where
      go (MealyArrow c) = MealyArrow $ rmap (bimap go f) c

  lmap f = go
    where
      go (MealyArrow c) = MealyArrow $ dimap f (first go) c
  dimap f g = go
    where
      go (MealyArrow c) = MealyArrow $ dimap f (bimap go g) c

-- | Constructing monadic xmealy arrows
newtype MealyF a b x = MealyF (a -> x,b)
instance Functor (MealyF a b) where
  fmap f (MealyF x) = MealyF $ first (f .) x

type MB a b = FreeT (MealyF a b)


yieldMB :: Monad m => b -> MB a b m a
yieldMB b = FreeT $ return $ Free $ MealyF (return,b)
finishMB ::Monad m => b -> MB a b m Void
finishMB b = fix (yieldMB b >>)


-- | Use yieldMB and finishMB and yieldMB and make sure to never
-- return.
mkMealy :: (ArrowFunctor c,Monad (ArrFunctor c))
        => (a -> MB a b (ArrFunctor c) Void)
        -> MealyArrow c a b
mkMealy mb = MealyArrow $ fromKleisli $ \a -> runFreeT (mb a)
  >>= \(Free (MealyF (mb',b))) -> return (mkMealy mb',b)

mealyScan :: Monad m => MealyArrow (Kleisli m) a b -> [a] -> m [b]
mealyScan _ [] = return []
mealyScan (MealyArrow (Kleisli m)) (a:as)  = do
  (nxt,b) <- m a
  (b:) <$> mealyScan nxt as

mealyLift :: Profunctor c => c a b -> MealyArrow c a b
mealyLift c = res
  where
    res = MealyArrow $ rmap (res,) c

-- | A mealy wrapped in a functor could become a mealy. The first
-- iteration of the mealy maching also includes the (ArrFunctor c)
-- effects but the rest are just normal.
squashMealy
  :: (ArrowFunctor c,Monad (ArrFunctor c))
  => ArrFunctor c (MealyArrow c a b)
  -> MealyArrow c a b
squashMealy m = MealyArrow $ fromKleisli $ \a -> do
  MealyArrow m' <- m
  toKleisli m' a
