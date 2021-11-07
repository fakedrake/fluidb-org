module Data.Utils.MinElem
  (MinElem(..)) where

class Ord a => MinElem a where
  minElem :: a
