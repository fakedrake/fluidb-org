module Data.Utils.AShowDebug (traceAShow) where

import Data.Utils.Debug
import Data.Utils.AShow

traceAShow :: AShow a => String -> a -> a
traceAShow msg a = trace (msg ++ ashow a) a
