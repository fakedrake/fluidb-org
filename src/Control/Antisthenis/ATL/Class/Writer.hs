{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE TypeFamilies           #-}

module Control.Antisthenis.ATL.Class.Writer
  (ArrowWriter(..)
  ,arrTell
  ,arrTell2
  ,arrListen
  ,arrListen2
  ,arrPass) where

import           Control.Arrow

arrTell :: ArrowWriter c => c (AWriterW c) ()
arrTell = arrCoListen' $ arr $ \w -> (w,())
arrTell2 :: (ArrowWriter c,ArrowWriter (AWriterArr c),Monoid (AWriterW c))
         => c (AWriterW (AWriterArr c)) ()
arrTell2 = arrCoListen' $ arrCoListen' $ arr $ \w -> (w,(mempty,()))
arrListen :: ArrowWriter c => c a b -> c a (AWriterW c,b)
arrListen c = arrCoListen' $ arrListen' c >>> arr (\(w,b) -> (w,(w,b)))
arrListen2
  :: (ArrowWriter c,ArrowWriter (AWriterArr c))
  => c a b
  -> c a (AWriterW (AWriterArr c),b)
arrListen2 c =
  arrCoListen'
  $ arrCoListen'
  $ arrListen' (arrListen' c)
  >>> arr (\(w0,(w1,b)) -> (w0,(w1,(w0,b))))
arrPass :: ArrowWriter c => c a (AWriterW c -> AWriterW c,b) -> c a b
arrPass c = arrCoListen' $ arrListen' c >>> arr (\(w,(f,b)) -> (f w,b))


class (Arrow c,Arrow (AWriterArr c)) => ArrowWriter c where
  type AWriterArr c :: * -> * -> *
  type AWriterW c :: *
  arrListen' :: c a b -> AWriterArr c a (AWriterW c,b)
  arrCoListen' :: AWriterArr c a (AWriterW c,b) -> c a b
