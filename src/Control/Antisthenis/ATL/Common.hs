module Control.Antisthenis.ATL.Common ((>>>)) where

import qualified Control.Category as C
infixr 1 >>>
(>>>) :: C.Category c => c a b -> c b c' -> c a c'
a >>> b = b C.. a
{-# INLINE (>>>) #-}
