{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
module Data.Query.GenericAlgebra
  ( Algebra(..)
  , WithOperator(..)
  , ArityToOp
  , glensOp
  , GArity
  ) where

import           Data.Proxy
import           Data.Query.GOperator
import           GHC.Generics
import           GHC.TypeLits

class GAlgebra a where
  type AlgType a :: * -> *

  gReconstruct
    :: Applicative m
    => (s -> m (AlgType a s'))
    -> (AlgType a s -> m (AlgType a s'))
    -> a s
    -> Either (m (AlgType a s')) (m (a s'))

class GAtomic a where
  gAtom :: s -> a s

class Algebra (a :: * -> *) where
  atom :: s -> a s
  default atom :: (Generic1 a,GAtomic (Rep1 a)) => s -> a s
  atom = to1 . gAtom

  qmapA1'
    :: Applicative m => (s -> m (a s')) -> (a s -> m (a s')) -> a s -> m (a s')
  default qmapA1'
    :: (Applicative m,Generic1 a,GAlgebra (Rep1 a),AlgType (Rep1 a) ~ a)
    => (s -> m (a s'))
    -> (a s -> m (a s'))
    -> a s
    -> m (a s')
  qmapA1' sfn afn e = either id (to1 <$>) $ gReconstruct sfn afn (from1 e)


type GArity n a = (KnownNat n,  KnownNat (ArityToDepth n (Rep1 a)))
class WithOperator (n :: Nat) (a :: * -> *) where
  -- | Try `ArityToOp n (Rep1 a)`
  type ArityToOperator n a :: *
  lensOp :: forall s m . Functor m =>
            Proxy n
         -> (forall k . m k)
         -> (ArityToOperator n a -> m (ArityToOperator n a))
         -> a s -> m (a s)
  default lensOp :: forall s m .
                    (GArity n a,
                     Functor m, Generic1 a, GOpClass (Rep1 a) 1,
                     ArityToOp n (Rep1 a) ~ ArityToOperator n a) =>
                    Proxy n
                 -> (forall k . m k)
                 -> (ArityToOperator n a -> m (ArityToOperator n a))
                 -> a s -> m (a s)
  lensOp p t fn e = to1 <$> glensOp p t fn (from1 e)

-- ATOMIC
instance GAtomic (a :+: b) => GAtomic (M1 D m (a :+: b)) where
  gAtom = M1 . gAtom

instance GAtomic (x' :+: x'') => GAtomic (M1 C m (x :*: y) :+: (x' :+: x'')) where
  gAtom = R1 . gAtom

instance GAtomic (M1 C m'' (a :*: b) :+: SymRec m m') where
  gAtom = R1 . M1 . M1 . Par1

instance GAtomic (SymRec m m' :+: rest) where
  gAtom = L1 . M1 . M1 . Par1

-- # PRODUCT TYPES
-- The first argument is an operator
type OpRec m o = M1 S m (Rec0 o)
instance (GAlgebra rest, AlgType (M1 C m' (OpRec m o :*: rest)) ~ at, at ~ AlgType rest) =>
         GAlgebra (M1 C m' (OpRec m o :*: rest)) where
  type AlgType (M1 C m' (OpRec m o :*: rest)) = AlgType rest
  gReconstruct sfn fn (M1 (o :*: rst)) = ((M1 . (coerce' o :*:)) <$>) <$> gReconstruct sfn fn rst where
    coerce' (M1 (K1 op)) = M1 (K1 op)

-- The next algebraic types are recursive
type SubRec m a = M1 S m (Rec1 a)
instance (GAlgebra (SubRec m x), GAlgebra y, AlgType y ~ al, al ~ AlgType (SubRec m x)) => GAlgebra (SubRec m x :*: y) where
  type AlgType (SubRec m x :*: y) = AlgType (SubRec m x)
  gReconstruct sfn fn (x :*: y) = do
    l <- gReconstruct sfn fn x
    r <- gReconstruct sfn fn y
    return $ (:*:) <$> l <*> r

instance GAlgebra (SubRec m x) where
  type AlgType (SubRec m x) = x
  gReconstruct _ fn (M1 (Rec1 x)) = Right $ M1 . Rec1 <$> fn x

-- # SUM TYPES
type SymRec m m' = (M1 C m (M1 S m' Par1))
-- An algebra has ONLY one way of lifting symbols
instance (GAlgebra (x :*: y), GAlgebra rest, AlgType rest ~ AlgType (x :*: y)) =>
         GAlgebra ((x :*: y) :+: rest) where
  type AlgType ((x :*: y) :+: rest) = AlgType (x :*: y)
  gReconstruct sf af (L1 a) = (L1 <$>) <$> gReconstruct sf af a
  gReconstruct sf af (R1 a) = (R1 <$>) <$> gReconstruct sf af a

-- SymRecs are not algebras because we can't deduce AlgType from it alone
instance (GAlgebra rest) => GAlgebra (SymRec m m' :+: rest) where
  type AlgType (SymRec m m' :+: rest) = AlgType rest
  gReconstruct sf _ (L1 (M1 (M1 (Par1 a)))) = Left $ sf a
  gReconstruct sf af (R1 a) = (R1 <$>) <$> gReconstruct sf af a

-- If we find a product we must immediately deduce algtype
instance (GAlgebra (M1 C m (a :*: b)), GAlgebra (a' :+: b'),
          AlgType (M1 C m (a :*: b)) ~ AlgType (a' :+: b')) =>
         GAlgebra (M1 C m (a :*: b) :+: (a' :+: b')) where
  type AlgType (M1 C m (a :*: b) :+: (a' :+: b')) = AlgType (M1 C m (a :*: b))
  gReconstruct sf af (L1 a) = (L1 <$>) <$> gReconstruct sf af a
  gReconstruct sf af (R1 a) = (R1 <$>) <$> gReconstruct sf af a

-- Symrescs will probably be last
instance (GAlgebra (M1 C m'' (a :*: b))) =>
         GAlgebra (M1 C m'' (a :*: b) :+: SymRec m m') where
  type AlgType (M1 C m'' (a :*: b) :+: SymRec m m') = AlgType (M1 C m'' (a :*: b))
  gReconstruct sf _ (R1 (M1 (M1 (Par1 a)))) = Left $ sf a
  gReconstruct sf af (L1 a) = (L1 <$>) <$> gReconstruct sf af a

-- If the final two are products
instance (GAlgebra (M1 C m' (a' :*: b')), GAlgebra (M1 C m (a :*: b)),
          AlgType (M1 C m (a :*: b)) ~ AlgType (M1 C m' (a' :*: b'))) =>
         GAlgebra (M1 C m (a :*: b) :+: M1 C m' (a' :*: b')) where
  type AlgType (M1 C m (a :*: b) :+: M1 C m' (a' :*: b')) = AlgType (M1 C m (a :*: b))
  gReconstruct sf af (L1 a) = (L1 <$>) <$> gReconstruct sf af a
  gReconstruct sf af (R1 a) = (R1 <$>) <$> gReconstruct sf af a

-- Algebra MUST be a sum type
instance GAlgebra (a :+: b) => GAlgebra (M1 D m (a :+: b)) where
  type AlgType (M1 D m (a :+: b)) = AlgType (a :+: b)
  gReconstruct sf af (M1 a) = (M1 <$>) <$> gReconstruct sf af a
