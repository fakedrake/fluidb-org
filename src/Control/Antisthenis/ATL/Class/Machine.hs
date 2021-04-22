{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TypeFamilies           #-}

module Control.Antisthenis.ATL.Class.Machine (ArrowMachine(..)) where

import           Control.Arrow

class (ArrowChoice (AMachineArr c),ArrowChoice (AMachineArr c),ArrowChoice c)
  => ArrowMachine c where
  -- We need to be able to traverse this
  type AMachineArr c :: * -> * -> *

  type AMachineNxtArr c :: * -> * -> *

  telescope :: c a b -> AMachineArr c a (AMachineNxtArr c a b,b)
  untelescope :: AMachineArr c a (AMachineNxtArr c a b,b) -> c a b
