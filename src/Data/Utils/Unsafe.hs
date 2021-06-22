{-# LANGUAGE LambdaCase #-}
module Data.Utils.Unsafe (safeTail,headErr,fromJustErr,foldr1Unsafe) where

import           GHC.Stack
safeTail :: [a] -> [a]
safeTail = \case
  []   -> []
  _:xs -> xs
headErr :: HasCallStack => [a] -> a
headErr = \case
  []  -> error "Head on empty list"
  x:_ -> x
fromJustErr :: HasCallStack => Maybe a -> a
fromJustErr = \case {Nothing -> error "From just failed."; Just x -> x}

foldr1Unsafe :: HasCallStack =>  (a -> a -> a) -> [a] -> a
foldr1Unsafe _ [] = error "foldr1 got an empty list."
foldr1Unsafe f as = foldr1 f as
