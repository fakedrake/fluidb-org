{-# LANGUAGE Arrows              #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
module Control.Antisthenis.ATL.Transformers.Moore
  (MooreCat(..)
  ,hoistMoore
  ,mooreBatch
  ,mkMooreCat
  ,mealyBatchC
  ,mooreBatchC
  ,mooreFoldInputs
  ,loopMooreCat) where

import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Arrow
import           Data.Profunctor

data MooreCat c a b = MooreMech b (MealyArrow c a b)

-- |
mealyBatchC
  :: forall a a' b b' c .
  (ArrowChoice c,ArrowApply c)
  => b
  -> c (a',b) (Either a b')
  -> MealyArrow c a b
  -> MealyArrow c a' b'
mealyBatchC b0 f = go b0
  where
    go :: b -> MealyArrow c a b -> MealyArrow c a' b'
    go b (MealyArrow c) = MealyArrow $ proc a' -> do
      e <- f -< (a',b)
      case e of
        Left a -> do
          (nxt,bNew) <- c -< a
          app -< (runMealyArrow $ go bNew nxt,a')
        Right b' -> returnA -< (go b $ MealyArrow c,b')

mealyBatch
  :: forall a a' b b' c .
  (ArrowChoice c,ArrowApply c)
  => b
  -> ((a',b) -> Either a b')
  -> MealyArrow c a b
  -> MealyArrow c a' b'
mealyBatch b0 f = go b0
  where
    go :: b -> MealyArrow c a b -> MealyArrow c a' b'
    go b (MealyArrow c) = MealyArrow $ proc a' -> do
      case f (a',b) of
        Left a -> do
          (nxt,bNew) <- c -< a
          app -< (runMealyArrow $ go bNew nxt,a')
        Right b' -> returnA -< (go b $ MealyArrow c,b')

mooreBatch
  :: forall c a a' b b' .
  (ArrowChoice c,ArrowApply c)
  => ((a',b) -> Either a b')
  -> MooreCat c a b
  -> MealyArrow c a' b'
mooreBatch f = \case
  MooreMech b mealy -> mealyBatch b f mealy

-- | Moore batching is a techinique for repetition of the arrow. See
-- `mealyBatchC`
mooreBatchC
  :: forall c a a' b b' .
  (ArrowChoice c,ArrowApply c,Profunctor c)
  => c (a',b) (Either a b')
  -> MooreCat c a b
  -> MealyArrow c a' b'
mooreBatchC c (MooreMech b mealy) = mealyBatchC b c mealy

mooreFoldInputs
  :: Arrow c => (a -> x -> x) -> x -> MooreCat c a b -> MooreCat c a (x,b)
mooreFoldInputs f i (MooreMech b m) = MooreMech (i,b) $ mealyFoldInputs f i m

mealyFoldInputs
  :: Arrow c => (a -> x -> x) -> x -> MealyArrow c a b -> MealyArrow c a (x,b)
mealyFoldInputs f i (MealyArrow m0) = MealyArrow $ proc a0 -> do
  (nxt,b) <- m0 -< a0
  returnA -< (go i nxt,(i,b))
  where
    go i' (MealyArrow m) = MealyArrow $ proc a -> do
      let x = f a i'
      (nxt,b) <- m -< a
      returnA -< (go x nxt,(x,b))


mkMooreCat :: b -> MealyArrow c a b -> MooreCat c a b
mkMooreCat = MooreMech

loopMooreCat :: (Profunctor c,Arrow c) => MooreCat c (a,d) (b,d) -> MooreCat c a b
loopMooreCat = \case
  MooreMech (b,d) f -> MooreMech b $ dimap (,d) fst f

instance Profunctor c => Profunctor (MooreCat c) where
  dimap f g (MooreMech b m) = MooreMech (g b) $ dimap f g m
  lmap f (MooreMech b m) = MooreMech b $ lmap f m
  rmap f (MooreMech b m) = MooreMech (f b) $ rmap f m


hoistMoore
  :: Profunctor c'
  => (b -> b')
  -> (c a (MealyArrow c a b,b) -> c' a' (MealyArrow c a b,b'))
  -> MooreCat c a b
  -> MooreCat c' a' b'
hoistMoore modb f (MooreMech x c) = MooreMech (modb x) $ hoistMealy f c
