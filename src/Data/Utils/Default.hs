{-# LANGUAGE DefaultSignatures    #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Data.Utils.Default (Default(..)) where

import           Data.Functor.Identity
import qualified Data.HashMap.Lazy     as HM
import qualified Data.HashSet          as HS
import           Data.IntMap
import           Data.IntSet
import qualified Data.List.NonEmpty    as NEL
import           GHC.Compact
import           GHC.Generics
import           System.IO.Unsafe

-- | A class for types with a default value.
class Default a where
  def :: a
  default def :: (Generic a,GDefault (Rep a)) => a
  def = to gDef

instance Default a => Default (Compact a) where
  def = unsafePerformIO $ compact def
instance Default Int where def = 0
instance Default Integer where def = 0
instance Default Double where def = 0
instance Default (Maybe a) where def = Nothing
instance Default Bool where def = False
instance Default () where def = mempty
instance Default [a] where def = mempty
instance Default a => Default (NEL.NonEmpty a) where def = def NEL.:| def
instance (Default a, Default b) => Default (a, b) where def = (def, def)
instance (Default a, Default b, Default c) => Default (a, b, c) where def = (def, def, def)
instance (Default a, Default b, Default c, Default d) => Default (a, b, c, d) where def = (def, def, def, def)
instance (Default a, Default b, Default c, Default d, Default e) => Default (a, b, c, d, e) where def = (def, def, def, def, def)
instance (Default a, Default b, Default c, Default d, Default e, Default f) => Default (a, b, c, d, e, f) where def = (def, def, def, def, def, def)
instance (Default a, Default b, Default c, Default d, Default e, Default f, Default g) => Default (a, b, c, d, e, f, g) where def = (def, def, def, def, def, def, def)
instance Default (IntMap v)   where def = mempty
instance Default IntSet       where def = mempty

instance Default (HM.HashMap k v) where def = HM.empty
instance Default (HS.HashSet v)  where def = HS.empty
instance Default a => Default (Identity a) where def = Identity def
instance Default a => Default (Either e a) where def = Right def

-- Generic Default for default implementation
class GDefault f where
    gDef :: f a

instance GDefault U1 where
    gDef = U1

instance (Datatype d, GDefault a) => GDefault (D1 d a) where
   gDef = M1 gDef

instance (Constructor c, GDefault a) => GDefault (C1 c a) where
   gDef = M1 gDef

instance (Selector s, GDefault a) => GDefault (S1 s a) where
   gDef = M1 gDef

instance (Default a) => GDefault (K1 i a) where
   gDef = K1 def

instance (GDefault a, GDefault b) => GDefault (a :*: b) where
   gDef = gDef :*: gDef

instance (HasRec a, GDefault a, GDefault b) => GDefault (a :+: b) where
   gDef = if hasRec' (gDef :: a p) then R1 gDef else L1 gDef

-- | We use 'HasRec' to check for recursion in the structure. This is used
-- to avoid selecting a recursive branch in the sum case for 'Empty'.
class HasRec a where
  hasRec' :: a x -> Bool
  hasRec' _ = False

instance HasRec V1
instance HasRec U1

instance (HasRec a) => HasRec (M1 i j a) where
  hasRec' (M1 x) = (hasRec' x)

instance (HasRec a, HasRec b) => HasRec (a :+: b) where
  hasRec' (L1 x) = hasRec' x
  hasRec' (R1 x) = hasRec' x

instance (HasRec a, HasRec b)
  => HasRec (a :*: b) where
  hasRec' (a :*: b) = hasRec' a || hasRec' b

instance HasRec (Rec0 b) where
  hasRec' (K1 _) = True
