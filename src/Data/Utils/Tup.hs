{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveTraversable #-}
module Data.Utils.Tup
  (Tup3(..)
  ,Tup2(..)
  ,fst3
  ,snd3
  ,trd3
  ,first3
  ,second3
  ,third3
  ,fstTwo3
  ,fst4
  ,snd4
  ,trd4
  ,fourth4
  ,fstTwo4
  ,fstThree4
  ,swap
  ,swapTup2) where

import           Data.Utils.Hashable
import           GHC.Generics

data Tup2 a = Tup2 a a
  deriving (Show, Eq, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Tup2 a)
instance Applicative Tup2 where
  pure x = Tup2 x x
  Tup2 f f' <*> Tup2 x x' = Tup2 (f x) (f' x')

data Tup3 a = Tup3 a a a
  deriving (Show, Eq, Functor, Foldable, Traversable, Generic)
instance Hashable a => Hashable (Tup3 a)
instance Applicative Tup3 where
  pure x = Tup3 x x x
  Tup3 f f' f'' <*> Tup3 x x' x'' = Tup3 (f x) (f' x') (f'' x'')

fst3 :: (a, b, c) -> a
fst3 (x, _, _) = x
snd3 :: (a, b, c) -> b
snd3 (_, x, _) = x
trd3 :: (a, b, c) -> c
trd3 (_, _, x) = x
first3 :: (t -> a) -> (t, b, c) -> (a, b, c)
first3 f (a,b,c) = (f a,b,c)
second3 :: (t -> b) -> (a, t, c) -> (a, b, c)
second3 f (a,b,c) = (a,f b,c)
third3 :: (t -> c) -> (a, b, t) -> (a, b, c)
third3 f (a,b,c) = (a,b,f c)
fstTwo3 :: (a, b, c) -> (a, b)
fstTwo3 (a,b,_) = (a,b)
fst4 :: (a, b, c, d) -> a
fst4 (x, _, _,_) = x
snd4 :: (a, b, c, d) -> b
snd4 (_, x, _, _) = x
trd4 :: (a, b, c, d) -> c
trd4 (_, _, x, _) = x
fourth4 :: (a, b, c, d) -> d
fourth4 (_, _, _, d) = d
fstTwo4 :: (a, b, c,d) -> (a, b)
fstTwo4 (a,b,_,_) = (a,b)
fstThree4 :: (a, b, c, d) -> (a, b, c)
fstThree4 (a,b,c,_) = (a,b,c)
swap :: (a,b) -> (b,a)
swap (a,b) = (b,a)
swapTup2 :: Tup2 a -> Tup2 a
swapTup2 (Tup2 a b) = Tup2 b a
