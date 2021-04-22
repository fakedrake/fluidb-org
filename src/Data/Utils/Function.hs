module Data.Utils.Function ((...),(....)) where

infixr 1 ...
(...) :: (b -> c) -> (a -> a1 -> b) -> a -> a1 -> c
(...) = (.).(.)

infixr 1 ....
(....) :: (b -> c) -> (a -> a1 -> a2 -> b) -> a -> a1 -> a2 -> c
(....) = (.).(.).(.)
