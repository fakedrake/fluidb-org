{-# LANGUAGE TypeFamilyDependencies #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -Wno-name-shadowing -Wno-missing-pattern-synonym-signatures #-}
module FluiDB.SumTypes
  ( p0
  , p1
  , p2
  , p3
  , p4
  , G(..)
  , pattern S0
  , pattern S1
  , pattern S2
  , pattern S3
  , pattern SR
  , (:+)
  , (:+:)
  , (:+>)
  , elift
  , elift1
  , epluck
  , extract
  , (<->)
  , estash
  , eunstash
  , lestash
  , leunstash
  , distribute
  , pattern L
  , pattern R
  , Fin
  , CType(..)
  , HeadLen
  , LExtendable(..)
  , N(..)
  , Head
  , Tail
  , associate2
  , associate3
  , dissociate3
  , commute
  , esquash
  , ersquash
  ) where
import GHC.TypeLits
import Data.Proxy
import Data.Void
import Control.Monad

class KnownN (n :: N) where
  nVal :: Proxy n -> Integer
instance KnownN 'Z where
  nVal _ = 0
instance KnownN n => KnownN ('S n) where
  nVal _ = 1 + nVal (Proxy :: Proxy n)

newtype CType x = CType {unCType :: x} deriving (Functor, Show, Eq, Ord)
infixr 6 :+:
type a :+: b = Either a (CType b)
infixr 7 :+
type a :+ b = Either a b
infixr 7 :+>
type a :+> b = a :+: Fin b
type Fin a = Either a Void

data N = S N | Z

class G (i :: N) x where
  type IType i x
  gen :: Proxy i -> IType i x -> x

instance G 'Z (a :+: b) where
  type IType 'Z (a :+: b) = a
  gen _ = S0

instance G 'Z (Fin a) where
  type IType 'Z (Fin a) = a
  gen _ = Left

instance G i b => G ('S i) (a :+: b) where
  type IType ('S i) (a :+: b) = IType i b
  gen :: forall (i :: N) a b . G i b =>
         Proxy ('S i) -> IType ('S i) (a :+: b) -> (a :+: b)
  gen _ x = Right $ CType $ gen (Proxy :: Proxy i) x

p0 :: Proxy 'Z
p0 = Proxy
p1 :: Proxy ('S 'Z)
p1 = Proxy
p2 :: Proxy ('S ('S 'Z))
p2 = Proxy
p3 :: Proxy ('S ('S ('S 'Z)))
p3 = Proxy
p4 :: Proxy ('S ('S ('S ('S 'Z))))
p4 = Proxy

pattern L x = Left x
pattern R x = Right x
pattern S0 :: x -> x :+ a
pattern S0 x = L x
pattern S1 :: x -> a :+: x :+ b
pattern S1 x = R (CType (L x))
pattern S2 :: x -> a :+: b :+: x :+ c
pattern S2 x = R (CType (R (CType (L x))))
pattern S3 :: x -> a :+: b :+: c :+: x :+ d
pattern S3 x = R (CType (R (CType (R (CType (L x))))))
pattern SR x = R (CType x)

-- Examples:
-- x' = gen p2 True :: Int :+: Char :+: Bool :+> String
-- x'' = gen p0 1 :: Int :+: Char :+: Bool :+> String
-- x''' = gen p1 'a' :: Int :+: Char :+: Bool :+> String
-- y' = S2 True :: Int :+: Char :+: Bool :+> String
-- y'' = S0 1 :: Int :+: Char :+: Bool :+> String
-- y''' = S1 'a' :: Int :+: Char :+: Bool :+> String
-- y'''' = S1 "hello" :: Int :+> String
-- Type errors:
-- x0 = gen p0 'a' :: Int :+: Char :+: Bool :+> String
-- x1 = gen p5 'a' :: Int :+: Char :+: Bool :+> String

-- | If a sum type is a subset of another sum type we can lift one to
-- the other. To lift between two sums their representations must have
-- the same ordering, ie type coproducts are not commutative as far as
-- we are concerned.
class LExtendable (hl :: N) x x' where
  -- | Lift a smaller sum type to a larger one.
  lelift :: Proxy hl -> x -> x'
  -- | Partial function to retrieve a smaller type from a larger one.
  lpluck :: Proxy hl -> x' -> Maybe x

instance LExtendable n x x' => LExtendable ('S n) x (a :+: x') where
  lelift _ x = (Right $ CType $ lelift (Proxy :: Proxy n) x)
  lpluck _ (Left _) = Nothing
  lpluck _ (Right (CType x)) = lpluck (Proxy :: Proxy n) x

instance LExtendable (HeadLen y y') y y' => LExtendable 'Z (x :+: y) (x :+: y') where
  lelift :: forall x y y' . LExtendable (HeadLen y y') y y' =>
            Proxy 'Z -> (x :+: y) -> (x :+: y')
  lelift _ (Left x) = Left x
  lelift _ (Right (CType x)) =
    Right $ CType $ lelift (Proxy :: Proxy (HeadLen y y')) x

  lpluck _ (Left x) = Just (Left x)
  lpluck _ (Right (CType x)) =
    (Right . CType) <$> lpluck (Proxy :: Proxy (HeadLen y y')) x

instance LExtendable 'Z (Fin x) (Fin x) where
  lelift _ = id
  lpluck _ (Left x) = Just (Left x)
  lpluck _ (Right _) = error $ "Encountered void"

instance LExtendable 'Z (Fin x) (x :+: y) where
  lelift _ (Left x) = Left x
  lelift _ (Right _) = error "Should be void"
  lpluck _ (Left x) = Just (Left x)
  lpluck _ (Right _) = Nothing

type HeadLen x y = HeadLen0 x y x y
-- | Just so we can report the error as early as possible.
type family HeadLen0 l r x x' = (r' :: N) where
  HeadLen0 l r (x :+: y) (x :+: z) = 'Z
  HeadLen0 l r (x :+: y) (z :+: w) = 'S (HeadLen0 l r (x :+: y) w)
  HeadLen0 l r (Fin x) (x :+: y) = 'Z
  HeadLen0 l r (Fin x) (y :+: z) = 'S (HeadLen0 l r (Fin x) z)
  HeadLen0 l r (Fin x) (Fin x) = 'Z
  HeadLen0 l r x y = TypeError (
    'Text "Sum type"
    ':$$: 'ShowType l
    ':$$: 'Text "\tnot subtype of"
    ':$$: 'ShowType r
    ':$$: 'Text "(We performed greedy matching so check the entire terms)")

lestash ::  forall sub sup (n :: N) . LExtendable n (Fin sub) sup =>
           Proxy n -> sup -> Either sup sub
lestash p x = maybe (Left x) (Right . fromLeft) $ lpluck p x where
  fromLeft :: Either a Void -> a
  fromLeft (Left x) = x
  fromLeft (Right _) = error "Void"
leunstash :: forall sub sup (n :: N) . LExtendable n (Fin sub) sup =>
            Proxy n -> Either sup sub -> sup
leunstash _ (Left x) = x
leunstash p (Right x) = lelift p (Left x :: Fin sub)

-- | Lift a sum type to a superset sum type.
elift :: forall x y . LExtendable (HeadLen x y) x y => x -> y
elift = lelift (Proxy :: Proxy (HeadLen x y))

-- | Lift a sum type to a superset sum type.
elift1 :: forall x y . LExtendable (HeadLen (Fin x) y) (Fin x) y => x -> y
elift1 = elift . (S0 :: e -> Fin e)

-- | Lower a sum type to a subset of that type. If the value is not in
-- the subset of types, Nothing.
epluck :: forall x y . LExtendable (HeadLen x y) x y => y -> Maybe x
epluck = lpluck (Proxy :: Proxy (HeadLen x y))

-- | If the instance of a sum type is a particular type pull it out,
-- otherwise return the type.
estash :: forall sub sup . LExtendable (HeadLen (Fin sub) sup) (Fin sub) sup =>
          sup -> Either sup sub
estash = lestash (Proxy :: Proxy (HeadLen (Fin sub) sup))

-- | Inverse of stashing.
eunstash :: forall sub sup . LExtendable (HeadLen (Fin sub) sup) (Fin sub) sup =>
            Either sup sub -> sup
eunstash = leunstash (Proxy :: Proxy (HeadLen (Fin sub) sup))

-- Good
-- x = elift (S1 'c' :: Int :+: Char :+> Bool) :: Int :+: Integer :+: Char :+: String :+: Bool :+> Proxy Char
-- Error
-- x = elift (S1 'c' :: String :+: Char :+> Bool) :: Int :+: Integer :+: Char :+: String :+: Bool :+> Proxy Char


-- | Split a sum type to head and tail
class Extractable head rest s | s -> head , s -> rest where
  extract :: (head -> t) -> (rest -> t) -> s -> t
  consl :: head -> s
  consr :: rest -> s

instance Extractable a (b :+: c) (a :+: (b :+: c)) where
  extract f _ (Left x) = f x
  extract _ f (Right (CType x)) = f x
  consl = Left
  consr = Right . CType

instance Extractable a b (a :+> b) where
  extract f _ (Left x) = f x
  extract _ g (Right (CType (Left x))) = g x
  extract _ _ (Right (CType (Right _))) = error "Void value"  -- Void
  consl = Left
  consr = Right . CType . Left

infixr 6 <->
-- | A convenient extracting combinator:
--
-- @
-- f :: a -> s
-- g :: b -> s
-- h :: c -> s
-- i :: d -> s
--  f <-> g <-> h <-> i ::
-- (a :+: b :+: c :+> d) -> s
-- @
(<->) :: Extractable head rest s =>
         (head -> t) -> (rest -> t) -> s -> t
a <-> b = extract a b

type family Head x
type instance Head (x :+ y) = x
type family Tail x
type instance Tail (x :+: y) = y

-- If all of them are on the left
distribute :: forall t h r x x' .
             (Traversable t, Extractable h r x, Extractable (t h) (t r) x') =>
             t x -> Maybe x'
distribute x = gol x `mplus` gor x where
  gol :: t x -> Maybe x'
  gol = fmap consl . traverse (Just <-> const Nothing)
  gor :: t x -> Maybe x'
  gor = fmap consr . traverse (const Nothing <-> Just)

associate3 :: a :+: b :+: c :+ rest -> (a :+: b :+> c) :+ rest
associate3 (S0 x) = S0 (S0 x)
associate3 (S1 x) = S0 (S1 x)
associate3 (S2 x) = S0 (S2 x)
associate3 (Right (CType (Right (CType (Right x))))) = Right x
associate3 _ = error "Exhaustive"

dissociate3 :: (a :+: b :+> c) :+ rest -> a :+: b :+: c :+ rest
dissociate3 (S0 (S0 x)) = S0 x
dissociate3 (S0 (S1 x)) = S1 x
dissociate3 (S0 (S2 x)) = S2 x
dissociate3 (Right x) = Right (CType (Right (CType (Right x))))
dissociate3 _ = error "Exhaustive"

associate2 :: a :+: b :+ rest -> (a :+> b) :+ rest
associate2 (S0 x) = S0 (S0 x)
associate2 (S1 x) = S0 (S1 x)
associate2 (Right (CType (Right x))) = Right x
associate2 _ = error "Exhaustive"

commute :: a :+: b :+ rest -> b :+: a :+ rest
commute (S0 x) = S1 x
commute (S1 x) = S0 x
commute (Right (CType (Right x))) = Right (CType (Right x))
commute _ = error "Exhaustive"

esquash :: forall a t x . LExtendable (HeadLen (Fin a) t) (Fin a) t =>
         Either a (Either t x) -> Either t x
esquash = either (Left . elift1) id

ersquash :: forall a t x . LExtendable (HeadLen (Fin a) t) (Fin a) t =>
         Either t (Either a x) -> Either t x
ersquash = either Left $ either (Left . elift1) Right
