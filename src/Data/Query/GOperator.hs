{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds             #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Data.Query.GOperator
  ( gsetOp
  , glensOp
  , ggetOp
  , GOpClass(..)
  , ArityToOp
  , ArityToDepth
  ) where
import           Control.Monad
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Identity
import           Data.Functor.Const
import           Data.Proxy
import           Data.Type.Equality
import           Data.Void
import           GHC.Generics
import           GHC.TypeLits
import           Unsafe.Coerce

type family IfEq (a :: Nat) (b :: Nat) (t :: c) (e :: c) where
  IfEq n n t e = t
  IfEq a b t e = e

-- Use 1 indexing and reserve 0 for error
type OpRec m o = M1 S m (Rec0 o)
type family DepthToOp0 (maxN :: Nat) (curN :: Nat) (e :: * -> *) :: * where
  DepthToOp0 0 c r = Void
  DepthToOp0 n c (M1 D m' e) = DepthToOp0 n c e
  -- The last item
  DepthToOp0 n n (M1 C m' (OpRec m o :*: rest)) = o
  DepthToOp0 n c (M1 C m' (OpRec m o :*: rest)) = Void
  -- A recursive item
  DepthToOp0 n n (M1 C m' (OpRec m o :*: rest) :+: r) = o
  DepthToOp0 n c (M1 C m' (OpRec m o :*: rest) :+: r) = DepthToOp0 n (c+1) r
  -- A symbol item
  DepthToOp0 n n (r' :+: r) = Void
  DepthToOp0 n c (r' :+: r) = DepthToOp0 n (c+1) r
  DepthToOp0 n c m = Void

type DepthToOp n g = DepthToOp1 n 1 g

-- The arity of product type
type family Arity0 (c :: Nat) (e :: * -> *) :: Nat where
  Arity0 n (C1 m a) = Arity0 n a
  Arity0 n (a :*: b) = Arity0 (n+1) b
  Arity0 n a = n

type Arity e = Arity0 0 e

-- We use 1 indexing to reserve 0 for errors
type family ArityToDepth0 (ar :: Nat) (cd :: Nat) (e :: * -> *) :: Nat where
  ArityToDepth0 a d (M1 D m e) = ArityToDepth0 a d e
  ArityToDepth0 a d (o :+: r) = IfEq a (Arity o) d (ArityToDepth0 a (d+1) r)
  ArityToDepth0 a d o = IfEq a (Arity o) d 0

type ArityToDepth ar e = ArityToDepth0 ar 1 e
type ArityToOp a e = DepthToOp (ArityToDepth a e) e

class GOpClass a (c :: Nat) where
  type DepthToOp1 (n :: Nat) (c :: Nat) a
  glensOp0 :: forall m n s . (KnownNat n, KnownNat c, Functor m) =>
             Proxy n -> Proxy c ->
             (forall k . m k) ->
             (DepthToOp1 n c a -> m (DepthToOp1 n c a)) ->
              a s -> m (a s)
  glensOp0 _ _ term _ _ = term

instance GOpClass e c => GOpClass (M1 D m e) c where
  type DepthToOp1 n c (M1 D m e) = DepthToOp1 n c e
  glensOp0 :: forall n s maybe . (KnownNat n, KnownNat c, Functor maybe) =>
             Proxy n -> Proxy c ->
             (forall k . maybe k) ->
             (DepthToOp1 n c (M1 D m e) -> maybe (DepthToOp1 n c (M1 D m e))) ->
             (M1 D m e) s -> maybe (M1 D m e s)
  glensOp0 prxN prxC term f (M1 a) = M1 <$> glensOp0 prxN prxC term f a

-- The very last one.
instance GOpClass (M1 C m' (OpRec m o :*: rest)) c where
  type DepthToOp1 n c (M1 C m' (OpRec m o :*: rest)) = IfEq n c o Void
  glensOp0 prxN prxC term f (M1 ((M1 (K1 o)) :*: recur)) =
    case sameNat prxC prxN of
      Just Refl -> M1 . (:*: recur) . M1 . K1 <$> f o
      Nothing   ->  term

ifThenElse :: forall (a :: Nat) (b :: Nat) x l r.
              (KnownNat a, KnownNat b, x ~ IfEq a b l r) =>
              Proxy a -> Proxy b -> Proxy (x,l,r) -> Either (x :~: l) (x :~: r)
ifThenElse pa pb _ = case sameNat pa pb of
  Just Refl -> Left Refl
  Nothing   -> Right (unsafeCoerce Refl)

instance (GOpClass rest' (c+1), KnownNat (c+1)) => GOpClass (M1 C m' (OpRec m o :*: rest) :+: rest') c where
  type DepthToOp1 n c (M1 C m' (OpRec m o :*: rest) :+: rest') =
    IfEq n c o (DepthToOp1 n (c + 1) rest')

  glensOp0 :: forall maybe n s . (KnownNat n, KnownNat c, Functor maybe) =>
             Proxy n
             -> Proxy c
             -> (forall k . maybe k)
             -> (IfEq n c o (DepthToOp1 n (c+1) rest') ->
                maybe (IfEq n c o (DepthToOp1 n (c+1) rest')))
             -> (M1 C m' (OpRec m o :*: rest) :+: rest') s
             -> maybe ((M1 C m' (OpRec m o :*: rest) :+: rest') s)
  glensOp0 prxN prxC term f (L1 (M1 (M1 (K1 o) :*: recur))) = case sameNat prxC prxN of
    Nothing   -> term
    Just Refl -> L1 . M1 . (:*: recur) . M1 . K1 <$> f o
  glensOp0 prxN prxC term f (R1 next) = case ifThenElse prxN prxC (Proxy :: Proxy (DepthToOp1 n c (M1 C m' (OpRec m o :*: rest) :+: rest'), o, DepthToOp1 n (c + 1) rest')) of
    Left Refl -> term
    Right Refl -> R1 <$> glensOp0 prxN (incr prxC) term f next
      where incr :: Proxy a -> Proxy (a + 1)
            incr _ = Proxy

type SymRec m m' = (M1 C m (M1 S m' Par1))
instance GOpClass (SymRec m m') c where
  type DepthToOp1 n c (SymRec m m') = Void
  glensOp0 _ _ term _ _ = term

glensOp :: forall a s (n::Nat) (start :: Nat) (d :: Nat) m.
          (GOpClass a start, start ~ 1, Functor m,
           d ~ ArityToDepth n a, KnownNat n,
           KnownNat d) =>
          Proxy n
       -> (forall k . m k)
       -> (DepthToOp1 d start a -> m (DepthToOp1 d start a))
       -> a s -> m (a s)
glensOp _ = glensOp0 (Proxy :: Proxy d) (Proxy :: Proxy start)

ggetOp :: forall a s (n::Nat) (start :: Nat) m.
          (GOpClass a start, KnownNat n, start ~ 1, MonadPlus m) =>
          Proxy n -> a s -> m (DepthToOp1 n start a)
ggetOp n e = getConst $
  glensOp0 n (Proxy :: Proxy start) (Const mzero) (Const . return) e

gsetOp :: forall a s (n::Nat) (start :: Nat) m .
          (GOpClass a start, KnownNat n, start ~ 1, MonadPlus m) =>
          Proxy n -> (DepthToOp1 n start a -> m (DepthToOp1 n start a)) -> a s -> m (a s)
gsetOp n f e = runIdentityT $
  glensOp0 n (Proxy :: Proxy start) (lift mzero) (lift . f) e

-- Tests
-- zero :: forall e (n :: Nat) s . e s -> Proxy (ArityToDepth 0 (Rep1 e))
-- zero _ = Proxy
-- one :: forall e (n :: Nat) s . e s -> Proxy (ArityToDepth 1 (Rep1 e))
-- one _ = Proxy
-- two :: forall e (n :: Nat) s . e s -> Proxy (ArityToDepth 2 (Rep1 e))
-- two _ = Proxy

-- exp0 :: Maybe Void
-- exp0 = ggetOp (zero e) (from1 e) where e = (E2 EAdd (atom 'x') (atom 'y'))
-- exp1 :: Maybe UEOp
-- exp1 = ggetOp (one e) (from1 e) where e = (E1 EId (atom 'x'))
-- exp2 :: Maybe BEOp
-- exp2 = ggetOp (two e) (from1 e) where e = (E2 EAdd (atom 'x') (atom 'y'))
