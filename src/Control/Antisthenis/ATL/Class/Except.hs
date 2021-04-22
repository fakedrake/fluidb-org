{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TypeFamilies           #-}

module Control.Antisthenis.ATL.Class.Except (arrThrow,ArrowExcept(..)) where

import           Control.Arrow

class (Arrow c,Arrow (AExceptArr c)) => ArrowExcept c where
  type AExceptArr c :: * -> * -> *

  type AExceptE c :: *

  arrUnExcept :: c x y -> AExceptArr c x (Either (AExceptE c) y)
  arrMkExcept :: AExceptArr c x (Either (AExceptE c) y) -> c x y

arrThrow :: ArrowExcept c => c (AExceptE c) a
arrThrow = arrMkExcept $ arr Left
