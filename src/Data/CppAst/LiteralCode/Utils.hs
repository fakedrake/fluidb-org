{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns      #-}
module Data.CppAst.LiteralCode.Utils
  ( commaSeparate
  , hyperGraph
  , notImplementedError
  , splitWhen
  , cppTemplate
  , mapPair
  , gather
  ) where

import           Data.List
import qualified Data.Map  as DM
import           GHC.Exts

commaSeparate :: (Monoid a, IsString a) => [a] -> a
commaSeparate []     = mempty
commaSeparate (x:xs) = foldl (\ret s -> ret <> ", " <> s) x xs

-- | A sorted list of lists that represent sets of indexes that
-- contain the same value for a.
hyperGraph :: Ord a => [a] -> [(a, [Int])]
hyperGraph = sortWith snd . DM.elems . foldl f DM.empty . zip ([0 ..] :: [Int])
  where
    f hgmap (i,k) = DM.alter (Just . maybe (k,[i]) (appendToSnd i)) k hgmap
      where
        appendToSnd x (a,rest) = (a,x : rest)

notImplementedError :: a
notImplementedError = error "Functionality coming soon."

splitWhen :: (a -> Bool) -> [a] -> [[a]]
splitWhen f = go [] where
  go ret []     = [reverse ret]
  go ret (x:xs) = if f x then reverse (x:ret):go [] xs else go (x:ret) xs

cppTemplate :: (Monoid a, IsString a) => a -> [a] -> a
cppTemplate name args = name <> "<" <> commaSeparate args <> ">"

{-# INLINE mapPair #-}
mapPair :: (a -> c, b -> d) -> (a,b) -> (c,d)
mapPair ~(f,g) ~(a,b) = (f a, g b)

gather :: (a -> a -> Bool) -> [a] -> [[a]]
gather _ [] = []
gather cmp (x:xs) = x':gather cmp rest where
  ((x:) -> x', rest) = partition (cmp x) xs
