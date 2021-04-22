{-# LANGUAGE LambdaCase #-}
module Data.Utils.Unsafe (safeTail,headErr,fromJustErr) where

import GHC.Stack
safeTail :: [a] -> [a]
safeTail = \case
  [] -> []
  _:xs -> xs
headErr :: HasCallStack => [a] -> a
headErr = \case
  [] -> error "Head on empty list"
  x:_ -> x
fromJustErr :: HasCallStack => Maybe a -> a
fromJustErr = \case {Nothing -> error "From just failed."; Just x -> x}
