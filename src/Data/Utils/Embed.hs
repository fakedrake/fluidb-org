{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module Data.Utils.Embed (Embed(..)) where
import           Data.Coerce

class Embed a b where
  emb :: a -> b
  default emb :: Coercible a b => a -> b
  emb = coerce
