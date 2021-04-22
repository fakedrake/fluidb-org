{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TypeFamilies           #-}

module Control.Antisthenis.ATL.Class.Reader
  (ArrowReader(..)
  ,arrAsk
  ,arrLocal
  ,arrCoLocal) where

import           Control.Arrow

class (Arrow (AReaderArr c),Arrow c) => ArrowReader c where
  type AReaderArr c :: * -> * -> *
  type AReaderR c :: *
  arrLocal' :: c a b -> AReaderArr c (AReaderR c,a) b
  arrCoLocal' :: AReaderArr c (AReaderR c,a) b -> c a b

arrAsk :: ArrowReader c => c () (AReaderR c)
arrAsk = arrCoLocal' $ arr fst
{-# INLINE arrAsk #-}
arrLocal :: ArrowReader c => c x y -> c (AReaderR c,x) y
arrLocal c = arrCoLocal' $ arr snd >>> arrLocal' c
arrCoLocal :: ArrowReader c => c (AReaderR c,x) y -> c x y
arrCoLocal c = arrCoLocal' $ arr (\(r,x) -> (r,(r,x))) >>> arrLocal' c
