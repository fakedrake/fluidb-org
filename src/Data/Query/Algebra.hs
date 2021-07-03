{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveFoldable        #-}
{-# LANGUAGE DeriveFunctor         #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE DeriveTraversable     #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE LiberalTypeSynonyms   #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}
module Data.Query.Algebra
  ( WithOperator(..)
  , Algebra(..)
  , Query(..)
  , Expr(..)
  , Prop(..)
  , FlipQuery(..)
  , BQOp(..)
  , UQOp(..)
  , BEOp(..)
  , UEOp(..)
  , BPOp(..)
  , UPOp(..)
  , BROp(..)
  , Rel(..)
  , pattern And
  , pattern Not
  , pattern Or
  , pattern S
  , pattern J
  , AggrFunction(..)
  , ElemFunction(..)
  , Aggr(..)
  , propQnfAnd
  , querySchemaNaive
  , foldSchema
  , unAggr
  , unAggrSafe
  , aggrToProj
  , toAggr
  , coUQOp
  , traverseExpr
  , traverseRel
  , traverseProp
  , traverseExprAggr
  , traverseExpr'
  , traverseRel'
  , traverseProp'
  , traverseExprAggr'
  , zipA
  , zipA'
  , flatten
  , flatten'
  , qmapM
  , qmapM'
  , qmapA1
  , qmapBFS
  , qmap
  , qmap'
  , qmap1
  , getOp
  , one
  , two
  , three
  , algebraFoldMap
  , algebraTraverse
  , algebraFMap
  , algebraJoin
  , algebraBind
  , algebraReturn
  , algebraAp
  , prodAppend
  , algebraPFMap
  , algebraPFMap1
  , algebraPFMapSym
  , replaceProj
  , moduloOne
  , moduloOne2
  , maxDepth
  , exprLens
  , AlgebraSymbol
  , BinaryOperator(..)
  , exprInverse
  , InvClass(..)
  , ExprLike(..)
  , atom2
  , atom3
  ) where

import           Data.CppAst
import           Data.Functor.Classes
import qualified Data.List.NonEmpty          as NEL
import           Data.Query.GenericAlgebra
import           Data.String
import           Data.Utils.AShow
import           Data.Utils.Const
import           Data.Utils.Default
import           Data.Utils.Function
import           Data.Utils.Functors
import           Data.Utils.Unsafe
import           GHC.Generics                (Generic, Generic1, Rep1)

import           Control.Monad
import           Control.Monad.Identity
import           Control.Monad.State.Strict
import           Control.Monad.Writer.Strict
import           Data.Bifoldable
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.Maybe
import           Data.Utils.Hashable

import           Data.Proxy
import           GHC.TypeLits

-- Note that if a BQOp exposes columns on both sides except, QJoin and
-- QProd, the QNFQuery break.
data BQOp es =
  QProd
  | QJoin (Prop (Rel (Expr es)))
  | QLeftAntijoin (Prop (Rel (Expr es)))
  | QRightAntijoin (Prop (Rel (Expr es)))
  | QUnion
  -- Project the right query from the columns from the left one.
  --
  -- Q2 QProjQuery l r = Q1 (QProj $ colOf l)  r
  | QProjQuery
  -- All columns on the left should appear on the right too.
  --
  -- Q2 QDistinct l r == Q1 (QGroup (colsOf r) (colsOf l)) r
  | QDistinct
  deriving (Generic, Show, Eq, Functor, Foldable, Traversable, Read)
instance AShow e => AShow (BQOp e)
instance ARead e => ARead (BQOp e)

instance Hashable es => Hashable (BQOp es)
data UQOp es =
  QSel (Prop (Rel (Expr es)))
  | QGroup [(es, Expr (Aggr (Expr es)))] [Expr es]
  | QProj [(es, Expr es)]
  -- XXX: merge QSort and QLimit into top-k.
  | QSort [Expr es]
  | QLimit Int
  | QDrop Int
  deriving (Generic, Eq, Show, Functor, Foldable, Traversable, Read)
instance AShow e => AShow (UQOp e)
instance ARead e => ARead (UQOp e)

coUQOp :: Eq e => UQOp e -> [e] -> Maybe (UQOp e)
coUQOp = \case
  QDrop i -> const $ Just $ QLimit i
  QLimit i -> const $ Just $ QDrop i
  QSel p -> const $ Just $ QSel (P1 PNot p)
  QProj p -> Just
    . QProj
    . fmap (\x -> (x,E0 x))
    . filter (`notElem` mapMaybe (\case {(_,E0 e) -> Just e; _ -> Nothing}) p)
  QSort _ -> const Nothing
  QGroup _ _ -> const Nothing

instance (Hashable es) => Hashable (UQOp es)
data Query es qs = Q2 (BQOp es) !(Query es qs) !(Query es qs)
  | Q1 (UQOp es) !(Query es qs)
  | Q0 qs
  deriving (Generic, Generic1, Eq, Show, Functor,Foldable,Traversable)
instance Eq e => Eq1 (Query e) where
  liftEq eq = curry $ \case
    (Q2 o l r,Q2 o' l' r') -> o == o' && liftEq eq l l' && liftEq eq r r'
    (Q1 o q,Q1 o' q') -> o == o' && liftEq eq q q'
    (Q0 s,Q0 s') -> eq s s'
    _ -> False
pattern J :: Prop (Rel (Expr e)) -> Query e s -> Query e s -> Query e s
pattern J p q q' = Q2 (QJoin p) q q'
pattern S :: Prop (Rel (Expr e)) -> Query e s -> Query e s
pattern S p q = Q1 (QSel p) q
instance (AShow e, AShow s) => AShow (Query e s) where
  ashow' = \case
    J p q q' -> sexp "J" [ashow' p, ashow' q, ashow' q']
    S p q -> sexp "S" [ashow' p, ashow' q]
    q -> genericAShow' q

instance (ARead e, ARead (Prop (Rel (Expr e))), ARead s) => ARead (Query e s) where
  aread' = \case
    Sub [Sym "J", p, l, r] -> J <$> aread' p <*> aread' l <*> aread' r
    Sub [Sym "S", p, q] -> S <$> aread' p <*> aread' q
    q -> genericARead' q


instance Bifoldable Query where
  bifoldr fe fs c = \case
    Q2 o l r -> bifoldr fe fs (foldr fe (bifoldr fe fs c r) o) l
    Q1 o q -> bifoldr fe fs (foldr fe c o) q
    Q0 s -> fs s c

instance Bitraversable Query where
  bitraverse fe fs = go where
    go = \case
      Q2 o l r -> Q2 <$> traverse fe o <*> go l <*> go r
      Q1 o q -> Q1 <$> traverse fe o <*> go q
      Q0 s -> Q0 <$> fs s

instance (Hashable es, Hashable qs) => Hashable (Query es qs)
instance Bifunctor Query where
  second = fmap
  first f = unFlipQuery . fmap f . FlipQuery
instance Algebra (Query es)
instance Applicative (Query e) where
  (<*>) = ap
  pure = Q0
instance Monad (Query e) where
  q >>= f = case q of
    Q2 o l r -> Q2 o (l >>= f) (r >>= f)
    Q1 o l   -> Q1 o $ l >>= f
    Q0 s     -> f s
  {-# INLINE (>>=) #-}
  return = atom
instance GArity n (Query es) => WithOperator n (Query es) where
  type ArityToOperator n (Query es) = ArityToOp n (Rep1 (Query es))


foldSchema :: (sch -> sch -> sch)
           -> (Either [(e,Expr e)] [(e, Expr (Aggr (Expr e)))] -> sch)
           -> Query e sch -> sch
foldSchema app mkSch = recur where
  recur = \case
    Q2 o l r -> case o of
      QProd            -> recur l `app` recur r
      QJoin _          -> recur l `app` recur r
      QProjQuery       -> recur l
      QUnion           -> recur l
      QDistinct        -> recur r
      QLeftAntijoin _  -> recur l
      QRightAntijoin _ -> recur r
    Q1 o q -> case o of
      QProj ps    -> mkSch $ Left ps
      QGroup ps _ -> mkSch $ Right ps
      _           -> recur q
    Q0 s -> s

newtype FlipQuery s e = FlipQuery {unFlipQuery :: Query e s} deriving Generic
unFlipQFn :: (FlipQuery s1 e1 -> FlipQuery s e) -> Query e1 s1 -> Query e s
unFlipQFn f = unFlipQuery . f . FlipQuery

instance Functor (FlipQuery s) where
  fmap f (FlipQuery (Q2 op q q')) = FlipQuery $ Q2
    (f <$> op)
    (unFlipQFn (f <$>) q)
    (unFlipQFn (f <$>) q')
  fmap f (FlipQuery (Q1 op q)) = FlipQuery $ Q1
    (f <$> op)
    (unFlipQFn (f <$>) q)
  fmap _ (FlipQuery (Q0 s)) = FlipQuery $ Q0 s

instance Foldable (FlipQuery s) where
  foldMap f (FlipQuery (Q2 op q q')) = foldMap f op
    <> foldMap f (FlipQuery q)
    <> foldMap f (FlipQuery q')
  foldMap f (FlipQuery (Q1 op q)) = foldMap f op
    <> foldMap f (FlipQuery q)
  foldMap _ (FlipQuery (Q0 _)) = mempty

instance Traversable (FlipQuery s) where
  traverse f (FlipQuery (Q2 op q q')) = fmap FlipQuery $ Q2
    <$> traverse f op
    <*> (unFlipQuery <$> traverse f (FlipQuery q))
    <*> (unFlipQuery <$> traverse f (FlipQuery q'))
  traverse f (FlipQuery (Q1 op q)) = fmap FlipQuery $ Q1
    <$> traverse f op
    <*> (unFlipQuery <$> traverse f (FlipQuery q))
  traverse _ (FlipQuery (Q0 s)) = pure $ FlipQuery $ Q0 s

instance (Hashable e, Hashable s) => Hashable (FlipQuery s e)

-- Expressions
data BEOp =
  -- Boolean (truthy)
 EEq | ELike | EAnd | EOr | ENEq
  -- Numeric
  | EAdd | ESub | EMul | EDiv
  deriving (Generic, Eq, Show, Enum, Bounded, Ord, Read)

data ElemFunction = ExtractDay
                  | ExtractMonth
                  | ExtractYear
                  | Prefix Int
                  | Suffix Int
                  | SubSeq Int Int
                  | AssertLength Int
                  deriving (Eq, Show, Generic, Read)
instance Hashable ElemFunction
instance AShow ElemFunction
instance ARead ElemFunction
data Aggr e = NAggr AggrFunction e
  deriving (Eq, Generic, Generic1, Show,
            Functor, Foldable, Traversable, Read)
instance Applicative Aggr where
  NAggr _ f <*> NAggr o x = NAggr o $ f x
  pure = NAggr AggrFirst
instance Default e => Default (Aggr e)
instance Hashable e => Hashable (Aggr e)
instance AShow e => AShow (Aggr e)
instance ARead e => ARead (Aggr e)
toAggr :: e -> Aggr e
toAggr = NAggr AggrFirst

unAggr :: Aggr a -> a
unAggr (NAggr _ e) = e

unAggrSafe :: Aggr a -> Maybe a
unAggrSafe (NAggr o e) = case o of
  AggrFirst -> Just e
  _         -> Nothing

aggrToProj :: [(x, Expr (Aggr (Expr y)))] -> Maybe [(x, Expr y)]
aggrToProj = fmap (fmap (fmap join)) . traverse (traverse (traverse unAggrSafe))

data AggrFunction = AggrSum
  | AggrCount
  | AggrAvg
  | AggrMin
  | AggrMax
  | AggrFirst
  deriving (Eq, Show, Enum, Generic, Read)
instance Default AggrFunction where def = AggrFirst
instance Hashable AggrFunction
instance Hashable BEOp
instance AShow AggrFunction
instance ARead AggrFunction
instance AShow BEOp
instance ARead BEOp
data UEOp =
  EFun ElemFunction
  | ENot
  | EAbs
  | ESig
  | ENeg deriving (Generic, Eq, Show, Read)
instance Hashable UEOp
instance AShow UEOp
instance ARead UEOp
data Expr s = E2 BEOp !(Expr s) !(Expr s)
  | E1 UEOp !(Expr s)
  | E0 s
  deriving (Eq,Generic,Generic1,Show,Functor,Read,Foldable,Traversable)
instance Default a => Default (Expr a) where def = E0 def
instance Hashable s => Hashable (Expr s)
instance Algebra Expr
instance Applicative Expr where
  (<*>) = algebraAp
  pure = atom
instance Monad Expr where
  (>>=) = flip algebraBind
  return = atom

instance Num a => Num (Expr a) where
  (+) = E2 EAdd
  (-) = E2 ESub
  (*) = E2 EMul
  abs = E1 EAbs
  signum = E1 ESig
  fromInteger = E0 . fromInteger

instance GArity n Expr => WithOperator n Expr where
  type ArityToOperator n Expr = ArityToOp n (Rep1 Expr)

-- Relationships
data BROp = REq | RGt | RLt | RGe | RLe | RLike | RSubstring
  deriving (Eq, Generic, Show, Enum, Bounded, Read)
instance Hashable BROp
instance AShow BROp
instance ARead BROp
data Rel a = R2 BROp !(Rel a) !(Rel a) | R0 a
  deriving (Eq, Generic, Generic1, Show, Functor, Foldable, Traversable, Read)
instance AShow e => AShow (Rel e)
instance ARead e => ARead (Rel e)
instance Hashable a => Hashable (Rel a)
instance Algebra Rel
instance GArity n Rel => WithOperator n Rel where
  type ArityToOperator n Rel = ArityToOp n (Rep1 Rel)
instance Applicative Rel where
  (<*>) = algebraAp
  pure = atom
instance Monad Rel where
  (>>=) = flip algebraBind
  return = atom

-- Propositions
data BPOp = PAnd | POr deriving (Eq,Generic, Show, Enum, Bounded, Read)
instance Hashable BPOp
instance AShow BPOp
instance ARead BPOp
data UPOp = PNot deriving (Eq,Generic, Show, Enum, Bounded, Read)
instance AShow UPOp
instance ARead UPOp
instance Hashable UPOp
data Prop a = P2 BPOp !(Prop a) !(Prop a)
  | P1 UPOp !(Prop a)
  | P0 a deriving (Generic, Generic1, Show, Eq, Functor, Foldable, Traversable)

instance Hashable a => Hashable (Prop a)
instance Algebra Prop
instance GArity n Prop => WithOperator n Prop where
  type ArityToOperator n Prop = ArityToOp n (Rep1 Prop)
instance Applicative Prop where
  (<*>) = algebraAp
  pure = atom
instance Monad Prop where
  (>>=) = flip algebraBind
  return = atom
pattern Or :: Prop t -> Prop t -> Prop t
pattern Or e1 e2 = P2 POr e1 e2
pattern And :: Prop t -> Prop t -> Prop t
pattern And e1 e2 = P2 PAnd e1 e2
pattern Not :: Prop t -> Prop t
pattern Not e = P1 PNot e
instance AShow a => AShow (Prop (Rel a))
instance ARead a => ARead (Prop (Rel a))
instance AShow a => AShow (Expr a)
instance ARead a => ARead (Expr a)

-- FUNCTIONS
traverseProp' :: Applicative m =>
                ([Query e s] -> Prop (Rel (Expr e)) -> m (Prop (Rel (Expr e))))
              -> Query e s
              -> m (Query e s)
traverseProp' f = go where
  go = \case
    Q2 o l r -> Q2 <$> go' o <*> go l <*> go r where
      go' o' = case o' of
        QJoin p          -> QJoin <$> f [l, r] p
        QLeftAntijoin p  -> QLeftAntijoin <$> f [l,r] p
        QRightAntijoin p -> QRightAntijoin <$> f [l,r] p
        _                -> pure o'
    Q1 o q -> Q1 <$> go' o <*> go q where
      go' o' = case o' of
        QSel p -> QSel <$> f [q] p
        _      -> pure o'
    Q0 s -> pure $ Q0 s

traverseProp :: Applicative m =>
               (Prop (Rel (Expr e)) -> m (Prop (Rel (Expr e))))
             -> Query e s
             -> m (Query e s)
traverseProp = traverseProp' . const

traverseRel' :: Applicative m =>
               ([Query e s] -> Rel (Expr e) -> m (Rel (Expr e)))
             -> Query e s
             -> m (Query e s)
traverseRel' fr = traverseProp' $ traverse . fr

traverseRel :: Applicative m =>
               (Rel (Expr e) -> m (Rel (Expr e)))
             -> Query e s
             -> m (Query e s)
traverseRel = traverseRel' . const

traverseExpr' :: Applicative m =>
               ([Query e s] -> Expr e -> m (Expr e))
             -> Query e s
             -> m (Query e s)
traverseExpr' fe = go where
  f = traverse2 . fe
  go = \case
    Q2 o l r -> Q2 <$> go' o <*> go l <*> go r where
      go' o' = case o' of
        QJoin p          -> QJoin <$> f [l, r] p
        QLeftAntijoin p  -> QLeftAntijoin <$> f [l, r] p
        QRightAntijoin p -> QRightAntijoin <$> f [l, r] p
        _                -> pure o'
    Q1 o q -> Q1 <$> go' o <*> go q where
      go' o' = case o' of
        QProj p    -> QProj <$> traverse2 (fe [q]) p
        QSort p    -> QSort <$> traverse (fe [q]) p
        QGroup p g -> QGroup <$> traverse4 (fe [q]) p <*> traverse (fe [q]) g
        QSel p     -> QSel <$> f [q] p
        _          -> pure o'
    Q0 s -> pure $ Q0 s
traverseExpr :: Applicative m =>
               (Expr e -> m (Expr e))
             -> Query e s
             -> m (Query e s)
traverseExpr = traverseExpr' . const

traverseExprAggr' :: Applicative m =>
                   ([Query e s] -> Expr (Aggr (Expr e)) -> m (Expr (Aggr (Expr e))))
                 -> Query e s
                 -> m (Query e s)
traverseExprAggr' fe = go where
  go = \case
    Q1 o q -> Q1 <$> go' o <*> go q where
      go' o' = case o' of
        QGroup p g -> QGroup <$> traverse2 (fe [q]) p <*> pure g
        _          -> pure o'
    q -> pure q

traverseExprAggr :: Applicative m =>
                   (Expr (Aggr (Expr e)) -> m (Expr (Aggr (Expr e))))
                 -> Query e s
                 -> m (Query e s)
traverseExprAggr = traverseExprAggr' . const


qmapA1 :: (Algebra a, Applicative m) => (a s -> m (a s)) -> a s -> m (a s)
qmapA1 = qmapA1' $ pure . atom

-- | Post-order rewrite: all children are processed before the parent
qmapM' :: (Algebra a, Monad m) => (a s -> m (a s)) -> a s -> m (a s)
qmapM' fn q = qmapA1 (qmapM' fn) q >>= fn

-- | Pre-order rewrite: parents are processed before descending in the
-- children.
qmapM :: (Algebra a, Monad m) => (a s -> m (a s)) -> a s -> m (a s)
qmapM fn q = fn q >>= qmapA1 (qmapM fn)

data DidItRun = ItRan | DidntRun deriving Eq
-- | Treverse the Query in a BFS manner each time potentially changing
-- it. To do this we compose @qmapA1@ recursively around fn. To
-- terminate we use a @StateT@ monad instead of the original monad to
-- count the numver of time @fn@ was applied in each iteration. When
-- fn was applied 0 times we stop.
qmapMBFS :: forall m a s. (Monad m, Algebra a) => (a s -> m (a s)) -> a s -> m (a s)
qmapMBFS _fn _q = evalStateT (loop fnInc _q) DidntRun
  where
    fnInc :: a s -> StateT DidItRun m (a s)
    fnInc q = put ItRan >> lift (_fn q)
    loop :: (a s -> StateT DidItRun m (a s)) -> a s -> StateT DidItRun m (a s)
    loop fn q = do
      put DidntRun
      newq <- fn q
      didItRun <- get
      if didItRun == DidntRun then return newq else loop (qmapA1 fn) newq

-- | Post-order rewrite.
qmap' :: Algebra a => (a s -> a s) -> a s -> a s
qmap' fn = runIdentity . qmapM' (Identity . fn)

-- | Pre-oreder rewrite.
qmap :: Algebra a => (a s -> a s) -> a s -> a s
qmap fn = runIdentity . qmapM (Identity . fn)

qmapBFS :: Algebra a => (a s -> a s) -> a s -> a s
qmapBFS fn = runIdentity . qmapMBFS (Identity . fn)

qmap1 :: Algebra a => (a s -> a s) -> a s -> a s
qmap1 fn = runIdentity . qmapA1 (Identity . fn)

flatten :: Algebra a => a s -> Either (a s) (a ())
flatten = qmapA1' (Left . atom) (const $ Right $ atom ())

flatten' :: Algebra a => a s -> a ()
flatten' = runIdentity . qmapA1' uni uni where uni = const $ Identity $ atom ()

zipA :: Algebra a => (a s -> a s' -> b) -> a s -> a s' -> [b]
zipA f q q' = zipWith f (collect q) (collect q') where
  collect :: Algebra a => a s -> [a s]
  collect q'' = execState (qmapA1' (return . atom) mod' q'') [] where
    mod' x' = modify (x':) >> return x'

zipA' :: (Algebra a, Applicative m) => (a s -> a s' -> m [b]) -> a s -> a s' -> m [b]
zipA' f q = fmap join . traverse id . zipA f q

getOp :: forall (n :: Nat) e s m . (WithOperator n e, MonadPlus m) =>
         Proxy n -> e s -> m (ArityToOperator n e)
getOp n e = getConst $ lensOp
  n
  (Const mzero)
  (Const . return)
  e

one :: Proxy 1
one = Proxy
two :: Proxy 2
two = Proxy
three :: Proxy 3
three = Proxy

-- Algebras are unofficially traverable, foldable, functors,
-- applicatives and monads. Below are the imoplementations of anything
-- necessary for that.
algebraTraverse :: (Applicative m, Algebra a) => (s -> m s') -> a s -> m (a s')
algebraTraverse f = qmapA1' (fmap atom . f) (algebraTraverse f)
algebraFoldMap :: (Monoid m, Algebra a) => (s -> m) -> a s -> m
algebraFoldMap f = snd . runWriter . go where
  go = qmapA1' (\s -> tell (f s) >> return (atom s)) go

-- Functorial
algebraFMap :: Algebra a => (s -> s') -> a s -> a s'
algebraFMap f = runIdentity . qmapA1' (return . atom . f) (return . algebraFMap f)

-- Applicative
algebraAp :: Algebra a => a (s -> s') -> a s -> a s'
algebraAp m1 m2 = (`algebraFMap` m2) `algebraBind` m1

-- Monadic operations
algebraJoin :: Algebra a => a (a s) -> a s
algebraJoin = algebraFMap unatom . runIdentity . go where
  go :: Algebra a => a (a s) -> Identity (a (a s))
  go = qmapA1' (pure . algebraFMap atom) go
  unatom = fromJustErr . getFirst . algebraFoldMap (First . Just)

algebraReturn :: Algebra a => s -> a s
algebraReturn = atom

algebraBind :: Algebra a => (s -> a s') -> a s -> a s'
algebraBind a b = algebraJoin $ algebraFMap a b

-- | Append products of queries maintaining shallow left. This is not
-- a redex.
prodAppend :: Query e s -> Query e s -> Query e s
prodAppend (Q2 QProd q q') (Q2 QProd q'' q''') =
  foldr prodAppend q [q',q'',q''']
prodAppend (Q2 QProd q q') q'' = q `prodAppend` (q' `prodAppend` q'')
prodAppend q (Q2 QProd q' q'') = Q2 QProd q (q' `prodAppend` q'')
prodAppend q q' = Q2 QProd q q'

algebraPFMap :: (Applicative m, Algebra a) => (a s -> m (a s)) -> a s -> m (a s)
algebraPFMap = algebraPFMapSym (pure . atom)

-- | Apply on terms that have ONLY sybols
algebraPFMapSym :: (Applicative m, Algebra a) =>
                  (s -> m (a s')) -> (a s -> m (a s')) -> a s -> m (a s')
algebraPFMapSym sf f = go where
  go q = if not $ null $ qmapA1' (\x -> [atom x]) (const []) q
         then f q
         else qmapA1' sf go q

-- | Apply on terms that have AT LEAST ONE sybol. If the function
-- inflates the symbols they are inflated until they can't be anymore.
algebraPFMap1 :: (Monad m, Algebra a) => (a s -> m (a s)) -> a s -> m (a s)
algebraPFMap1 f = go where
  go q = (if hasSymbol then f q else pure q) >>= qmapA1' (return . atom) go
    where hasSymbol = isNothing $ qmapA1' (const Nothing) Just q

type family AlgebraSymbol (x :: *) where
  AlgebraSymbol (a s) = s

-- | Lookup the symbols of an expression and replace them with the
-- projection.
replaceProj :: Eq e => [(e, Expr (Aggr (Expr e)))] -> Expr e -> Expr e
replaceProj proj = (>>= (\x -> fromMaybe (E0 x)
                          $ x `lookup` (second (>>= unAggr) <$> proj)))
class BinaryOperator o where
  opPrescedence :: o -> Int

instance BinaryOperator BEOp where
  opPrescedence o = case o of
    EMul  -> 0
    EDiv  -> 0
    EAdd  -> 1
    ESub  -> 1
    EEq   -> 2
    ELike -> 2
    EAnd  -> 3
    EOr   -> 3
    ENEq  -> 3

instance BinaryOperator BROp where
  opPrescedence o = case o of
    RSubstring -> 0
    _          -> 1

instance BinaryOperator BPOp where
  opPrescedence _ = 0

data InvClass a = ENoInv | EMulti | EConst | EInv a deriving (Functor, Show)
instance Monad InvClass where
  return = EInv
  x >>= f = case x of
    EInv a -> f a
    ENoInv -> ENoInv
    EConst -> EConst
    EMulti -> EMulti

instance Applicative InvClass where
  pure = return
  (<*>) = ap

-- | Given E[x] and a way to detect the `x` symbol, try to get the
-- E^(-1). The function provided must trigger for *exactly* one symbol
-- on the expression (eg it won't work for @x+x@). Also only
-- arithmentic operations are permitted.
exprInverse :: forall e m . Applicative m =>
              (e -> m Bool) -> Expr e -> m (InvClass (e, e -> Expr e))
exprInverse isSym = fmap3 (. E0) . go
  where
    go :: Expr e -> m (InvClass (e, Expr e -> Expr e))
    go = \case
      E0 n -> isSym n <&> \p -> if p
                              then return (n, id)
                              else EConst
      E1 o q -> fmap3 (. E1 o) $ go q
      E2 o a b -> ((,) <$> go a <*> go b) <&> \case
        (EMulti, _) -> EMulti
        (ENoInv, _) -> ENoInv
        (_, ENoInv) -> ENoInv
        (_, EMulti) -> EMulti
        (EConst, EConst) -> EConst
        (EInv _, EInv _) -> EMulti
        (EConst, EInv (e, ib)) -> fmap (e,) $ case o of -- A o f(x)
          EAdd -> EInv $ ib . (\x -> E2 ESub x a)
          ESub -> EInv $ ib . E1 ENeg . (\x -> E2 ESub x a)
          EMul -> EInv $ ib . (\x -> E2 EDiv x a)
          EDiv -> EInv $ ib . E2 EDiv a
          _    -> ENoInv
        (EInv (e, ia), EConst) -> fmap (e,) $ case o of  -- f(x) o B
          EAdd -> EInv $ ia . (\x -> E2 ESub x b)
          ESub -> EInv $ ia . (\x -> E2 EAdd x b)
          EMul -> EInv $ ia . (\x -> E2 EDiv x b)
          EDiv -> EInv $ ia . (\x -> E2 EMul x b)
          _    -> ENoInv

class ExprLike e a where
  asExpr :: Expr e -> a
instance ExprLike e (Expr e) where
  asExpr = id
instance ExprLike e (Expr (Aggr (Expr e))) where
  asExpr = E0 . toAggr
instance ExprLike e (Expr (Either x e)) where
  asExpr = fmap Right


-- | The left hand side has some notion of a free variable that is
-- infective and can not be mixed with other free variables. The right
-- hand side has no free variables and can safely be used with
-- functions of free variables.
data WithOneFree s e = WithOneFree [(e, s)] [e] deriving Functor
instance Applicative (WithOneFree s) where
  WithOneFree frf nfrf <*> WithOneFree fra nfra =
    WithOneFree withFree withoutFree
    where
      withFree = withFree1 ++ withFree2
      withFree1 = do
        (f, carry) <- frf
        v <- nfra
        return (f v, carry)
      withFree2 = do
        (v, carry) <- fra
        f <- nfrf
        return (f v, carry)
      withoutFree = nfrf <*> nfra
  pure x = WithOneFree [] [x]

instance Semigroup (WithOneFree s e) where
  WithOneFree x xs <> WithOneFree y ys =
    WithOneFree (x <> y) (xs <> ys)
instance Monoid (WithOneFree s e) where
  mempty = WithOneFree mempty mempty
frees :: WithOneFree s e -> [(e, s)]
frees (WithOneFree xs _) = xs

-- | Pulls each symbol out and serves it separately:
--
--  > [(f '_', s) | (f, s) <- moduloOne (P2 (PAnd) (P0 'a') (P0 'b'))]
--  [(P2 PAnd (P0 '_') (P0 'b'),'a'),
--   (P2 PAnd (P0 'a') (P0 '_'),'b')]
--
-- Also we have: [f s == p | (f, s) <- moduloOne p]
-- and: length (moduloOne p) == length (toList p)
moduloOne :: forall a s . Algebra a => a s -> [(s -> a s, s)]
moduloOne = fmap traverseFuncT . traverseFuncL . (frees ... untraversed)
  where
    untraversed :: a s -> s -> WithOneFree s (a s)
    untraversed orig sub = qmapA1' sym at orig where
      sym :: s -> WithOneFree s (a s)
      sym s = WithOneFree [(atom sub, s)] [atom s]
      at :: a s -> WithOneFree s (a s)
      at orig' = untraversed orig' sub
    -- | Do not use this lightly: this function only works if the
    -- structure of the list (ie which of the constructors is used) is
    -- independent of the argumetn.
    traverseFuncT :: (x -> (y, z)) -> (x -> y, z)
    traverseFuncT f = (fst . f, snd $ f undefined)
    traverseFuncL :: (x -> [y]) -> [x -> y]
    traverseFuncL f = case f undefined of
      []  -> []
      _:_ -> headErr . f:traverseFuncL (tail . f)


-- | See moduloOne
moduloOne2 :: (Algebra a, Algebra a') => a' (a s) -> [(s -> a' (a s), s)]
moduloOne2 r = do (fr, e) <- moduloOne r
                  (fe, s) <- moduloOne e
                  return (fr . fe, s)

maxDepth :: forall a s . Algebra a => a s -> Int
maxDepth = (`execState` 0) . go where
  go :: a s -> State Int (a s)
  go = qmapA1' (\r -> modify (+1) >> return (atom r)) recur where
      recur :: a s -> State Int (a s)
      recur r = do
        preDepth <- get
        let subDepth = go r `execState` 0
        put $ max preDepth (subDepth + 1)
        return r

-- | Lenses
subAlg :: Applicative m =>
         (e -> m e')
       -> (Expr e -> m (Expr e'))
       -> (Expr (Aggr (Expr e)) -> m (Expr (Aggr (Expr e'))))
       -> (Rel (Expr e) -> m (Rel (Expr e')))
       -> (Prop (Rel (Expr e)) -> m (Prop (Rel (Expr e'))))
       -> Query e s -> m (Query e' s)
subAlg f fe fa fr fp = \case
  Q2 o q0 q1 -> Q2 <$> bqop o <*> recur q0 <*> recur q1 where
    bqop = \case
      QProd -> pure QProd
      QUnion -> pure QUnion
      QJoin p -> QJoin <$> fp p
      QLeftAntijoin p -> QLeftAntijoin <$> fp p
      QRightAntijoin p -> QRightAntijoin <$> fp p
      QProjQuery -> pure QProjQuery
      QDistinct -> pure QDistinct
  Q1 o q  -> Q1 <$> uqop o <*> recur q where
    uqop = \case
      QSel p -> QSel <$> fp p
      QGroup prj grp ->
        QGroup <$> traverse (f `bitraverse` fa) prj <*> traverse fe grp
      QProj prj -> QProj <$> traverse (f `bitraverse` fe) prj
      QSort es -> QSort <$> traverse fe es
      QLimit i -> pure $ QLimit i
      QDrop i -> pure $ QDrop i
  Q0 s -> pure $ Q0 s
  where
    recur = subAlg f fe fa fr fp

exprLens :: Applicative m => (Expr e -> m (Expr e)) -> Query e s -> m (Query e s)
exprLens f = subAlg pure f (traverse2 f) (traverse f) (traverse2 f)

atom2 :: (Algebra a1, Algebra a2) => x -> a1 (a2 x)
atom2 = atom . atom
atom3 :: (Algebra a1, Algebra a2, Algebra a3) => x -> a1 (a2 (a3 x))
atom3 = atom . atom . atom


instance Read e => Read (Prop (Rel (Expr e))) where
  readsPrec d = readParen (d > app_prec) $ \r ->
    [(P2 o q q',s3) |
      ("P2",s0) <- lex r,
      (o,s1) <- readsPrec (app_prec+1) s0,
      (q,s2) <- readsPrec (app_prec+1) s1,
      (q',s3) <- readsPrec (app_prec+1) s2]
    ++
    [(P1 op q, s2) |
      ("P1",s0) <- lex r,
      (op,s1) <- readsPrec (app_prec+1) s0,
      (q,s2) <- readsPrec (app_prec+1) s1]
    ++
    [(P0 e, s1) |
      ("P0",s0) <- lex r,
      (e,s1) <- readsPrec (app_prec+1) s0]
    ++
    [(P2 o q q',s3) |
      ("P2",s0) <- lex r,
      (o,s1) <- readsPrec (app_prec+1) s0,
      (q,s2) <- readsPrec (app_prec+1) s1,
      (q',s3) <- readsPrec (app_prec+1) s2]
    ++
    [(And q q',s2) |
      ("And",s0) <- lex r,
      (q,s1) <- readsPrec (app_prec+1) s0,
      (q',s2) <- readsPrec (app_prec+1) s1]
    ++
    [(Or q q',s2) |
      ("Or",s0) <- lex r,
      (q,s1) <- readsPrec (app_prec+1) s0,
      (q',s2) <- readsPrec (app_prec+1) s1]
    ++
    [(Not p,s1) |
      ("Not",s0) <- lex r,
      (p,s1) <- readsPrec (app_prec+1) s0]

    where app_prec = 10

instance (Read e, Read s) => Read (Query e s) where
  readsPrec d = readParen (d > app_prec) $ \r ->
    [(J p q q',s3) |
      ("J",s0) <- lex r,
      (p,s1) <- readsPrec (app_prec+1) s0,
      (q,s2) <- readsPrec (app_prec+1) s1,
      (q',s3) <- readsPrec (app_prec+1) s2]
    ++
    [(S p q, s2) |
      ("S",s0) <- lex r,
      (p,s1) <- readsPrec (app_prec+1) s0,
      (q,s2) <- readsPrec (app_prec+1) s1]
    ++
    [(Q1 op q, s2) |
      ("Q2",s0) <- lex r,
      (op,s1) <- readsPrec (app_prec+1) s0,
      (q,s2) <- readsPrec (app_prec+1) s1]
    ++
    [(Q0 e, s1) |
      ("Q0",s0) <- lex r,
      (e,s1) <- readsPrec (app_prec+1) s0]
    ++
    [(Q2 o q q',s3) |
      ("Q2",s0) <- lex r,
      (o,s1) <- readsPrec (app_prec+1) s0,
      (q,s2) <- readsPrec (app_prec+1) s1,
      (q',s3) <- readsPrec (app_prec+1) s2]

    where app_prec = 10

querySchemaNaive :: Query e s
                 -> [Either s
                    (Either [(e,Expr e)] ([(e,Expr (Aggr (Expr e)))],[Expr e]),
                     Query e s)]
querySchemaNaive = \case
  Q0 s -> [Left s]
  Q1 o l -> case o of
    QProj p    -> [Right (Left p,Q1 (QProj p) l)]
    QGroup p e -> [Right (Right (p,e),Q1 (QGroup p e) l)]
    _          -> querySchemaNaive l
  Q2 o l r -> case o of
    QProd            -> querySchemaNaive l ++ querySchemaNaive r
    QJoin _          -> querySchemaNaive l ++ querySchemaNaive r
    QLeftAntijoin _  -> querySchemaNaive l
    QRightAntijoin _ -> querySchemaNaive r
    QUnion           -> querySchemaNaive l
    QProjQuery       -> querySchemaNaive l
    QDistinct        -> querySchemaNaive r


-- | Codegen
instance IsCode code => Codegen BEOp code where
  toCode = \case
    EAdd -> "+"
    ESub -> "-"
    EMul -> "*"
    EDiv -> "/"
    EEq -> "=="
    ENEq -> "!="
    EAnd -> "&&"
    EOr -> "||"
    ELike -> error "Like should be handled higher up"

instance IsCode code => Codegen BROp code where
  toCode = \case
    REq -> "=="
    RGt -> ">"
    RLt -> "<"
    RGe -> ">="
    RLe -> "<="
    RLike -> "like"
    RSubstring -> "substring"

instance IsCode code => Codegen BPOp code where
  toCode = \case
    PAnd -> "&&"
    POr -> "||"

instance IsCode code => Codegen AggrFunction code where
  toCode = \case
    AggrSum -> "sum"
    AggrCount -> "count"
    AggrAvg -> "avg"
    AggrMin -> "min"
    AggrMax -> "max"
    AggrFirst -> "first"

-- instance (IsCode code, Codegen e code) => Codegen (Aggr e) code where
--   toCode (NAggr af e) = toCode af <> "(" <> toCode e <> ")"

instance IsCode code => Codegen ElemFunction code where
  toCode = \case
    ExtractDay -> "extract_day"
    ExtractMonth -> "extract_month"
    ExtractYear -> "extract_year"
    Prefix i -> "prefix<" <> fromString (show i) <> ">"
    Suffix i -> "suffix<" <> fromString (show i) <> ">"
    SubSeq i j -> "subseq<" <> fromString (show i) <> ", " <>
                 fromString (show j) <> ">"
    AssertLength i -> "exact_length_or_null<" <> fromString (show i) <> ">"

instance IsCode code => Codegen UEOp code where
  toCode = \case
    ENeg -> "-"
    ENot -> "!"
    EFun f -> toCode f
    EAbs -> "abs"
    ESig -> "sig"

instance IsCode code => Codegen UPOp code where
  toCode PNot = "!"
propQnfAnd :: forall x . Eq x => Prop x -> NEL.NonEmpty (Prop x)
propQnfAnd = go where
  go x@(P0 _) = return x
  go x@(Not (P0 _)) = return x
  go (Not (Not x)) = go x
  go (Not (Or x y)) = go (Not x) <> go (Not y)
  go (Not (And x y)) = return $ Not (And x y)
  -- go (Not (And x y)) = go $
  --   Or (foldr1Unsafe And $ go $ Not x) (foldr1Unsafe And $ go $ Not y)
  go (And x y) = go x <> go y
  go p@(Or x y) = fromMaybe (return p) $ goOr (go x) (go y)
    where
      goOr :: NEL.NonEmpty (Prop x)
           -> NEL.NonEmpty (Prop x)
           -> Maybe (NEL.NonEmpty (Prop x))
      goOr ls rs = NEL.nonEmpty $ inters ++ rest
        where
          rest = case (NEL.filter (`notElem` rs) ls,nonIntersR) of
            ([],[])       -> []
            (l,[])        -> l
            ([],r)        -> r
            (l:ls',r:rs') -> [Or (foldr And l ls') (foldr And r rs')]
          (inters,nonIntersR) = NEL.partition (`elem` ls) rs
  go _ = error "Unreachable, the price for using patterns."
