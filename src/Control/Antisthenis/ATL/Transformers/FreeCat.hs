{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# LANGUAGE UndecidableInstances  #-}

module Control.Antisthenis.ATL.Transformers.FreeCat
  (FreeCat(..)
  ,FCNorm(..)
  ,evalFC
  ,evalFCSM
  ,evalFCNSM
  ,hoistFC
  ,runReaderArrowFC
  ,runExceptArrowFC
  ,runStateArrowFC
  ,liftFCN
  ,evalFCN
  ,liftFC) where

import           Control.Arrow
import           Control.Category
import           Data.Constraint                          (Dict (..))
import           Control.Antisthenis.ATL.Transformers.Except
import           Control.Antisthenis.ATL.Transformers.Reader
import           Control.Antisthenis.ATL.Transformers.SMArrow
import           Control.Antisthenis.ATL.Transformers.State
import           Prelude                                  hiding (id, (.))

-- | For some arrows (especially continuations and mealy) arr and
-- composition are extremely expensive. For that reason we want to do
-- as few of them as possible with minimal
data FreeCat :: (* -> * -> *) -> * -> * -> * where
  FCId :: FreeCat c a a
  FCComp :: Either (c a x) (a -> x) -> FreeCat c x b -> FreeCat c a b

-- Function that supports id
newtype a :-> b = MkIdF (Either (Dict (a ~ b)) (a -> b))
arr' :: Arrow c => (a :-> b) -> c a b
arr' (MkIdF (Left Dict)) = id
arr' (MkIdF (Right f))   = arr f
instance Category (:->) where
  id = MkIdF $ Left Dict
  MkIdF f0 . MkIdF g0 = MkIdF $ case (f0,g0) of
    (Left Dict,g)     -> g
    (f,Left Dict)     -> f
    (Right f,Right g) -> Right $ f . g
  {-# INLINE (.) #-}
  {-# INLINE id #-}

instance Arrow (:->) where
  arr f = MkIdF $ Right f
  first (MkIdF (Right f))   = MkIdF $ Right $ first f
  first (MkIdF (Left Dict)) = MkIdF $ Left Dict
  second (MkIdF (Right f))   = MkIdF $ Right $ second f
  second (MkIdF (Left Dict)) = MkIdF $ Left Dict
  MkIdF l0 &&& MkIdF r0 = MkIdF $ case (l0,r0) of
    (Left Dict,Left Dict) -> Right $ \x -> (x,x)
    (Left Dict,Right r)   -> Right $ \x -> (x,r x)
    (Right l,Left Dict)   -> Right $ \x -> (l x,x)
    (Right l,Right r)     -> Right $ \x -> (l x,r x)
  MkIdF l0 *** MkIdF r0 = MkIdF $ case (l0,r0) of
    (Left Dict,Left Dict) -> Left Dict
    (Left Dict,Right r)   -> Right $ second r
    (Right l,Left Dict)   -> Right $ first l
    (Right l,Right r)     -> Right $ l *** r
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (&&&) #-}
  {-# INLINE (***) #-}

instance ArrowChoice (:->) where
  left (MkIdF (Left (Dict))) = MkIdF $ Left Dict
  left (MkIdF (Right f))     = MkIdF $ Right $ left f
  right (MkIdF (Left (Dict))) = MkIdF $ Left Dict
  right (MkIdF (Right f))     = MkIdF $ Right $ right f
  MkIdF l0 +++ MkIdF r0 = MkIdF $ case (l0,r0) of
    (Left Dict,Left Dict) -> Left Dict
    (Left Dict,Right r)   -> Right $ right r
    (Right l,Left Dict)   -> Right $ left l
    (Right x,Right y)     -> Right $ x +++ y
  MkIdF l0 ||| MkIdF r0 = MkIdF $ case (l0,r0) of
    (Left Dict,Left Dict) -> Right $ \case {Left x -> x;Right x -> x}
    (Left Dict,Right r)   -> Right $ \case {Left x -> x;Right x -> r x}
    (Right l,Left Dict)   -> Right $ \case {Left x -> l x;Right x -> x}
    (Right l,Right r)     -> Right $ \case {Left x -> l x;Right x -> r x}
  {-# INLINE left #-}
  {-# INLINE right #-}
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}

data FCNorm :: (* -> * -> *) -> * -> * -> * where
  FCNFunc :: (a :-> b) -> FCNorm c a b
  FCNrm :: (a :-> x) -> c x y -> (y :-> b) -> FCNorm c a b

instance Arrow c => Arrow (FCNorm c) where
  arr f = FCNFunc $ arr f
  first (FCNFunc f)   = FCNFunc $ first f
  first (FCNrm f c g) = FCNrm (first f) (first c) (first g)
  second (FCNFunc f)   = FCNFunc $ second f
  second (FCNrm f c g) = FCNrm (second f) (second c) (second g)
  x &&& y = case (x,y) of
    (FCNFunc l,FCNFunc r)        -> FCNFunc $ l &&& r
    (FCNrm f' c' g',FCNrm f c g) -> FCNrm (f' &&& f) (c' *** c) (g' *** g)
    (FCNFunc l,FCNrm f c g)      -> FCNrm (l &&& f) (second c) (second g)
    (FCNrm f c g,FCNFunc r)      -> FCNrm (f &&& r) (first c) (first g)
  x *** y = case (x,y) of
    (FCNFunc l,FCNFunc r)        -> FCNFunc $ l *** r
    (FCNrm f' c' g',FCNrm f c g) -> FCNrm (f' *** f) (c' *** c) (g' *** g)
    (FCNFunc l,FCNrm f c g)      -> FCNrm (l *** f) (second c) (second g)
    (FCNrm f c g,FCNFunc r)      -> FCNrm (f *** r) (first c) (first g)
  {-# INLINE arr #-}
  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (&&&) #-}

instance ArrowChoice c => ArrowChoice (FCNorm c) where
  left = \case
    FCNFunc f -> FCNFunc $ left f
    FCNrm f c g -> FCNrm (left f) (left c) (left g)
  right = \case
    FCNFunc f -> FCNFunc $ right f
    FCNrm f c g -> FCNrm (right f) (right c) (right g)
  x +++ y = case (x,y) of
    (FCNFunc l,FCNFunc r)        -> FCNFunc $ l +++ r
    (FCNrm f' c' g',FCNrm f c g) -> FCNrm (f' +++ f) (c' +++ c) (g' +++ g)
    (FCNFunc l,FCNrm f c g)      -> FCNrm (l +++ f) (right c) (right g)
    (FCNrm f c g,FCNFunc r)      -> FCNrm (f +++ r) (left c) (left g)
  x ||| y = case (x,y) of
    (FCNFunc l,FCNFunc r)        -> FCNFunc $ l ||| r
    (FCNrm f' c' g',FCNrm f c g) -> FCNrm (f' +++ f) (c' +++ c) (g' ||| g)
    (FCNFunc l,FCNrm f c g)      -> FCNrm (l +++ f) (right c) (id ||| g)
    (FCNrm f c g,FCNFunc r)      -> FCNrm (f +++ r) (left c) (g ||| id)
  {-# INLINE left #-}
  {-# INLINE right #-}
  {-# INLINE (|||) #-}
  {-# INLINE (+++) #-}

instance Arrow c => Category (FCNorm c) where
  id = FCNFunc id
  f0 . g0 = case (f0,g0) of
    (FCNFunc f,FCNFunc g) -> FCNFunc $ f . g
    (FCNFunc g',FCNrm f c g) -> FCNrm f c (g' . g)
    (FCNrm f c g,FCNFunc f') -> FCNrm (f . f') c g
    (FCNrm f' c' g',FCNrm f c g)
      -> FCNrm f (case g >>> f' of
                     MkIdF (Left Dict) -> c >>> c'
                     MkIdF (Right f'') -> c >>> arr f'' >>> c') g'
  {-# INLINE (.) #-}
  {-# INLINE id #-}

evalFCN :: Arrow c => FCNorm c a b -> c a b
evalFCN (FCNFunc (MkIdF (Left Dict))) = id
evalFCN (FCNFunc (MkIdF (Right f))) = arr f
evalFCN (FCNrm (MkIdF f0) c (MkIdF g0)) = case (f0,g0) of
  (Left Dict,Left Dict) -> c
  (Left Dict,Right f)   -> c >>> arr f
  (Right f,Right g)     -> arr f >>> c >>> arr g
  (Right f,Left Dict)   -> arr f >>> c
liftFCN :: c a b -> FCNorm c a b
liftFCN c = FCNrm id c id


evalFC :: Arrow c => FreeCat c a b -> c a b
evalFC FCId            = id
evalFC (FCComp e FCId) = either id arr e
evalFC (FCComp e rest) = either id arr e >>> evalFC rest
liftFC :: c a b -> FreeCat c a b
liftFC c = FCComp (Left c) FCId
hoistFC :: (forall x y . c x y -> c' x y) -> FreeCat c a b -> FreeCat c' a b
hoistFC _ FCId                 = FCId
hoistFC f (FCComp (Right x) y) = FCComp (Right x) $ hoistFC f y
hoistFC f (FCComp (Left x) y)  = FCComp (Left $ f x) $ hoistFC f y

instance Category (FreeCat c) where
  id = FCId
  l . FCId = l
  r . FCComp car FCId = FCComp car r
  r . FCComp car cdr = FCComp car $ r . cdr
  {-# INLINE (.) #-}
  {-# INLINE id #-}

instance Arrow c => Arrow (FreeCat c) where
  arr f = FCComp (Right f) FCId
  first FCId = FCId
  first (FCComp car cdr) = FCComp (either (Left . first) (Right . first) car) $ first cdr
  second FCId = FCId
  second (FCComp car cdr) = FCComp (either (Left . second) (Right . second) car) $ second cdr
  FCComp (Right lcar) lcdr *** FCComp (Right rcar) rcdr =
    FCComp (Right $ lcar *** rcar) $ lcdr *** rcdr
  FCComp (Right lcar) lcdr *** rcdr = FCComp (Right $ first lcar) $ lcdr *** rcdr
  lcdr *** FCComp (Right rcar) rcdr = FCComp (Right $ second rcar) $ lcdr *** rcdr
  lcdr *** FCId = first lcdr
  FCId *** rcdr = second rcdr
  lcdr *** rcdr = first lcdr >>> second rcdr
  l &&& r = arr (\b -> (b,b)) >>> l *** r

  {-# INLINE first #-}
  {-# INLINE second #-}
  {-# INLINE (***) #-}
  {-# INLINE (&&&) #-}
  {-# INLINE arr #-}


instance ArrowChoice c => ArrowChoice (FreeCat c) where
  left FCId = FCId
  left (FCComp car cdr) =
    FCComp (either (Left . left) (Right . left) car) $ left cdr
  right FCId = FCId
  right (FCComp car cdr) =
    FCComp (either (Left . right) (Right . right) car) $ right cdr
  FCComp (Right lcar) lcdr +++ FCComp (Right rcar) rcdr =
    FCComp (Right $ lcar +++ rcar) $ lcdr +++ rcdr
  FCComp (Right lcar) lcdr +++ rcdr = FCComp (Right $ left lcar) $ lcdr +++ rcdr
  lcdr +++ FCComp (Right rcar) rcdr = FCComp (Right $ right rcar) $ lcdr +++ rcdr
  l +++ r = left l >>> right r
  FCComp (Right lcar) lcdr ||| FCComp (Right rcar) rcdr =
    FCComp (Right $ lcar +++ rcar) $ lcdr ||| rcdr
  FCComp (Right lcar) lcdr ||| rcdr = FCComp (Right $ left lcar) $ lcdr ||| rcdr
  lcdr ||| FCComp (Right rcar) rcdr = FCComp (Right $ right rcar) $ lcdr ||| rcdr
  l ||| r = l +++ r >>> arr (either id id)
  {-# INLINE left #-}
  {-# INLINE right #-}
  {-# INLINE (+++) #-}
  {-# INLINE (|||) #-}

runReaderArrowFC :: Arrow c => FreeCat (ReaderArrow r c) a b -> FreeCat c (r,a) b
runReaderArrowFC FCId = arr snd
runReaderArrowFC (FCComp car cdr) =
  FCComp (either (\(ReaderArrow x) -> Left $ arr fst &&& x) (Right . second) car)
  $ runReaderArrowFC cdr
runStateArrowFC :: Arrow c => FreeCat (StateArrow s c) a b -> FreeCat c (s,a) (s,b)
runStateArrowFC FCId = FCId
runStateArrowFC (FCComp car cdr) =
  FCComp (either (Left . runStateArrow) (Right . second) car)
  $ runStateArrowFC cdr
runExceptArrowFC :: ArrowChoice c => FreeCat (ExceptArrow e c) a b -> FreeCat c a (Either e b)
runExceptArrowFC FCId = arr Right
runExceptArrowFC (FCComp car cdr) =
  FCComp (either (Left . runExceptArrow) (Right . (Right .)) car)
  $ arr Left ||| runExceptArrowFC cdr
evalFCNSM :: ArrowApply c => FCNorm (SMArrow r c) a b -> SMArrow r c a b
evalFCNSM = \case
  FCNFunc f -> arr' f
  FCNrm (MkIdF f0) (SMArrow c) (MkIdF g0) -> case (f0,g0) of
    (Left Dict,Left Dict) -> SMArrow c
    (Right f,Left Dict)
      -> let fin = arr (first (arr f >>>))
         in SMArrow $ \exit -> arr f >>> c (fin >>> exit) >>> fin
    (Left Dict,Right g)
      -> let fin = arr ((>>> arr g) *** g)
         in SMArrow $ \exit -> c (fin >>> exit) >>> fin
    (Right f,Right g)
      -> let fin = arr ((\x -> arr f >>> x >>> arr g) *** g)
         in SMArrow $ \exit -> arr f >>> c (fin >>> exit) >>> fin


evalFCSM :: ArrowApply c => FreeCat (SMArrow r c) a b -> SMArrow r c a b
evalFCSM FCId = id
evalFCSM (FCComp (Left x) FCId) = x
evalFCSM cdr@(FCComp (Left _) _) = evalFCSM $ FCComp (Right id) cdr
evalFCSM (FCComp (Right x0) cdr) = goPre x0 cdr
  where
    goPre :: ArrowApply c
          => (a -> x)
          -> FreeCat (SMArrow r c) x b
          -> SMArrow r c a b
    goPre x = \case
      FCId -> arr x
      FCComp (Right x') rest -> goPre (x >>> x') rest
      FCComp (Left x') rest -> goPost x x' (Left Dict) rest
    goPost
      :: ArrowApply c
      => (a -> x)
      -> SMArrow r c x y
      -> Either (Dict (y ~ z)) (y -> z)
      -> FreeCat (SMArrow r c) z b
      -> SMArrow r c a b
    goPost pre cur post0 rest0 = case (post0,rest0) of
      (Left Dict,FCId) -> prePost (arr pre) cur $ arr id
      (Right f,FCId) -> prePost (arr pre) cur $ arr f
      (Left Dict,FCComp (Right post) rest)
        -> goPost pre cur (Right post) rest
      (Right post,FCComp (Right post') rest)
        -> goPost pre cur (Right $ post >>> post') rest
      (Left Dict,FCComp (Left cur') rest)
        -> goPost pre (cur >>> cur') (Left Dict) rest
      (Right post,FCComp (Left cur') rest)
        -> goPost pre (cur >>> arr post >>> cur') (Left Dict) rest

prePost :: Arrow c => c x y -> SMArrow r c y z -> c z w -> SMArrow r c x w
prePost xy yz' zw = go yz' where
  go (SMArrow yz) = SMArrow $ \exit -> xy >>> yz ((arr go *** zw) >>> exit) >>> (arr go *** zw)
