{-# LANGUAGE CPP                     #-}
{-# LANGUAGE ConstraintKinds         #-}
{-# LANGUAGE DefaultSignatures       #-}
{-# LANGUAGE DerivingVia             #-}
{-# LANGUAGE FlexibleContexts        #-}
{-# LANGUAGE FlexibleInstances       #-}
{-# LANGUAGE GADTs                   #-}
{-# LANGUAGE KindSignatures          #-}
{-# LANGUAGE LambdaCase              #-}
{-# LANGUAGE MultiParamTypeClasses   #-}
{-# LANGUAGE PartialTypeSignatures   #-}
{-# LANGUAGE RankNTypes              #-}
{-# LANGUAGE ScopedTypeVariables     #-}
{-# LANGUAGE StandaloneDeriving      #-}
{-# LANGUAGE TypeApplications        #-}
{-# LANGUAGE TypeFamilies            #-}
{-# LANGUAGE TypeOperators           #-}
{-# LANGUAGE UndecidableInstances    #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE ViewPatterns            #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
module Control.Antisthenis.ATL.Transformers.Eff () where

#if 0
-- This is some boilerplate to optilize the arrows online.

import           Control.Arrow
import qualified Control.Category                        as C
import           Data.Constraint                         hiding ((&&&), (***))
import           Data.Functor.Identity
import           Data.List.NonEmpty                      as NEL
import           Data.Maybe
import           Data.Proxy
import           Control.Antisthenis.ATL.Class.Machine
import           Control.Antisthenis.ATL.Class.Select
import           Control.Antisthenis.ATL.Class.State
import           Control.Antisthenis.ATL.Transformers.Mealy
import           Control.Antisthenis.ATL.Transformers.Select
import           Control.Antisthenis.ATL.Transformers.State

evalEArr :: EArr c a b -> c a b
evalEArr = go
  where
    go :: EArr c x y -> c x y
    go = \case
      ELeft x -> left $ go x
      ERight x -> right $ go x
      l :+++: r -> go l +++ go r
      l :|||: r -> go l ||| go r
      EFirst x -> first $ go x
      ESecond x -> second $ go x
      -- | Telescope will almost always be interacting with modify
      ETelescope x -> telescope (go x)
        >>> first (arr EVal)
      EUnTelescope x -> untelescope $ go x >>> first (arr go)
      EModify x -> arrModify $ go x
      ECoModify x -> arrCoModify $ go x
      l :>>>: r -> go l >>> go r
      l :&&&: r -> go l &&& go r
      l :***: r -> go l *** go r
      EArr x -> arr x
      EId -> C.id
      EApp -> go app
      EVal v -> v

-- Note that we are contraining the constructors so we can evaluate
-- anything.
data EArr :: (* -> * -> *) -> * -> * -> * where
  EVal :: c a b -> EArr c a b
  -- Category
  (:>>>:) :: ATL c => EArr c x y -> EArr c y z -> EArr c x z
  EId :: ATL c => EArr c a a
  -- ArrowApply
  EApp :: (ATL c,ArrowApply c) => EArr c (EArr c a b,a) b
  -- Arrow
  EArr :: ATL c => (a -> b) -> EArr c a b
  EFirst :: ATL c => EArr c a b -> EArr c (a,d) (b,d)
  ESecond :: ATL c => EArr c a b -> EArr c (d,a) (d,b)
  (:&&&:) :: ATL c => EArr c a b -> EArr c a d -> EArr c a (b,d)
  (:***:) :: ATL c => EArr c a b -> EArr c a' b' -> EArr c (a,a') (b,b')
  -- ArrowChoice
  ELeft :: (ATL c,ArrowChoice c)
    => EArr c a b
    -> EArr c (Either a d) (Either b d)
  ERight :: (ATL c,ArrowChoice c)
    => EArr c a b
    -> EArr c (Either d a) (Either d b)
  (:+++:) :: (ATL c,ArrowChoice c)
    => EArr c a b
    -> EArr c a' b'
    -> EArr c (Either a a') (Either b b')
  (:|||:) :: (ATL c,ArrowChoice c)
    => EArr c a b
    -> EArr c a' b
    -> EArr c (Either a a') b
  -- ArrowState
  EModify :: (ATL c,ArrowState c,ATL (AStateArr c))
    => EArr (AStateArr c) (AStateSt c,a) (AStateSt c,b)
    -> EArr c a b
  ECoModify :: (ATL c,ArrowState c,ATL (AStateArr c))
    => EArr c a b
    -> EArr (AStateArr c) (AStateSt c,a) (AStateSt c,b)
  -- ArrowExcept
  -- ArrowReader
  -- ArrowSelect
  -- ArrowMachine
  ETelescope :: (ATL c,ArrowMachine c,ATL (AMachineArr c))
    => EArr c a b
    -> EArr (AMachineArr c) a (EArr (AMachineNxtArr c) a b,b)
  EUnTelescope :: (ATL c,ArrowMachine c,ATL (AMachineArr c))
    => EArr (AMachineArr c) a (EArr (AMachineNxtArr c) a b,b)
    -> EArr c a b

eUnState
  :: forall c a b .
  (ATL c,ArrowState c)
  => EArr c a b
  -> Maybe (EArr (AStateArr c) a b)
eUnState = go
  where
    go :: EArr c x y -> Maybe (EArr (AStateArr c) x y)
    go x = atl $ case x of
      ELeft x        -> ch $ ELeft <$> go x
      ERight x       -> ch $ ERight <$> go x
      l :+++: r      -> ch $ (:+++:) <$> go l <*> go r
      l :|||: r      -> ch $ (:|||:) <$> go l <*> go r
      EFirst x       -> EFirst <$> go x
      ESecond x      -> ESecond <$> go x
      -- | Telescope will almost always be interacting with modify
      ETelescope _   -> Nothing
      EUnTelescope _ -> Nothing
      EModify _      -> Nothing
      ECoModify _    -> Nothing
      l :>>>: r      -> (:>>>:) <$> go l <*> go r
      l :&&&: r      -> (:&&&:) <$> go l <*> go r
      l :***: r      -> (:***:) <$> go l <*> go r
      EApp           -> Nothing
      EArr x         -> pure $ EArr x
      EId            -> pure EId
      EVal _         -> Nothing
      where
        ch :: ArrowChoice c
           => ((ArrowChoice (AStateArr c)) => Maybe x)
           -> Maybe x
        ch xM = do
          Dict <- atlState @c Proxy
          xM
        atl :: ((ATL (AStateArr c)) => Maybe x) -> Maybe x
        atl xM = do
          Dict <- atlState @c Proxy
          xM

instance AShow (EArr c a b) where ashow' = ashow3'
class AShow3 t where ashow3' :: t (c :: * -> * -> *) a b -> SExp
instance AShow3 EArr where
  ashow3' = \case
    EVal _ -> sexp "EVal" []
    EArr _ -> sexp "EArr" []
    EId -> Sym "EId"
    EApp -> Sym "EApp"
    ELeft x -> sexp "ELeft" [ashow3' x]
    ERight x -> sexp "ERight" [ashow3' x]
    EFirst x -> sexp "EFirst" [ashow3' x]
    ESecond x -> sexp "ESecond" [ashow3' x]
    ETelescope x -> sexp "ETelescope" [ashow3' x]
    EUnTelescope x -> sexp "EUnTelescope" [ashow3' x]
    EModify x -> sexp "EModify" [ashow3' x]
    ECoModify x -> sexp "ECoModify" [ashow3' x]
    l :>>>: r -> sexp ">>>" [ashow3' l,ashow3' r]
    l :+++: r -> sexp ":+++:" [ashow3' l,ashow3' r]
    l :|||: r -> sexp ":|||:" [ashow3' l,ashow3' r]
    l :&&&: r -> sexp ":&&&:" [ashow3' l,ashow3' r]
    l :***: r -> sexp ":***:" [ashow3' l,ashow3' r]

instance ATL c => C.Category (EArr c) where
  (.) = flip (:>>>:)
  id = EId
instance ATL c => Arrow (EArr c) where
  arr = EArr
  first = EFirst
  second = ESecond
  (***) = (:***:)
  (&&&) = (:&&&:)
instance (ATL c,ArrowChoice c) => ArrowChoice (EArr c) where
  left = ELeft
  right = ERight
  (|||) = (:|||:)
  (+++) = (:+++:)

class constr c => Sat constr c where
  indMech :: Proxy c -> Proxy constr -> Maybe (Dict (Sat constr (AMachineArr c)))
  indState :: Proxy c -> Proxy constr -> Maybe (Dict (Sat constr (AStateArr c)))

class Sat ATL c => ATL c where
  atlMech :: Proxy c -> Maybe (Dict (ATL (AMachineArr c)))
  atlState :: Proxy c -> Maybe (Dict (ATL (AStateArr c)))

instance (ArrowChoice c,ATL c) => ATL (EArr c) where
  atlMech _ = do
    Dict <- atlMech (Proxy :: Proxy c)
    return Dict
  atlState _ = do
    Dict <- atlState (Proxy :: Proxy c)
    return Dict

instance ATL c => ATL (StateArrow s c) where
  atlMech _ = do
    Dict <- atlMech (Proxy :: Proxy c)
    return Dict
  atlState _ = Just Dict

instance (ATL c,ArrowChoice c) => ATL (MealyArrow c) where
  atlMech _ = Just Dict
  atlState _ = do
    Dict <- atlState (Proxy :: Proxy c)
    return Dict
instance (ArrowApply c,ATL c,ArrowChoice c)
  => ATL (SelectArrow r c) where
  atlMech _ = Nothing
  atlState _ = do
    Dict <- atlState (Proxy :: Proxy c)
    return Dict

instance ATL (->) where
  atlMech _ = Nothing
  atlState _ = Nothing

modifySeq
  :: forall c x a b .
  ATL c
  => EArr c a x
  -> EArr c x b
  -> Maybe
    (Dict (ArrowState c,ATL (AStateArr c))
    ,EArr (AStateArr c) (AStateSt c,a) (AStateSt c,b))
modifySeq a0 b0 = case (a0,b0) of
  (EModify x,EModify y) -> return $ (Dict,x :>>>: y)
  (x,EModify y) -> do
    x <- eUnState x
    return $ (Dict,ESecond x :>>>: y)
  (EModify x,y) -> do
    y <- eUnState y
    return $ (Dict,x :>>>: ESecond y)
  _ -> Nothing

mapCompSeq :: (forall x y . c x y -> c' x y) -> CompSeq c a b -> CompSeq c' a b
mapCompSeq f = \case
  CSComp x xs -> CSComp (f x) $ mapCompSeq f xs
  CSUnit x -> CSUnit $ f x

lengthCompSeq :: CompSeq c a b -> Int
lengthCompSeq = \case
  CSComp _ xs -> 1 + lengthCompSeq xs
  CSUnit _ -> 1
optEArr :: forall c a b .ATL c => EArr c a b -> EArr c a b
optEArr = evalEArr . optEArr'
  where
    optEArr' :: ATL c' => EArr c' x y -> EArr (EArr c') x y
    optEArr' =
      compSeq . foldWindowCompSeq 100 optCons' . mapCompSeq go . asCompSeq
    optCons'
      :: ATL c'
      => EArr (EArr c') x y
      -> EArr (EArr c') y z
      -> Maybe (EArr (EArr c') x z)
    optCons' l r =
      case optCons (\l' r' -> fromMaybe (l' :>>>: r') $ optCons' l' r') l r of
        _ :>>>: _ -> Nothing
        x         -> Just x
    go :: forall c' x y . ATL c' => EArr c' x y -> EArr (EArr c') x y
    go ast = case ast of
      EFirst (EModify (go -> (EArr sasb))) -> EModify
        $ EArr (\(s,(a,d)) -> let (s',b) = sasb (s,a)
                              in (s',(b,d)))
      ELeft x -> ELeft $ go x
      ERight x -> ERight $ go x
      EFirst x -> EFirst $ go x
      ESecond x -> ESecond $ go x
      ETelescope x -> ETelescope (go x)
        :>>>: EFirst (arr evalEArr)
      EUnTelescope x -> untelescope $ go x >>> first (arr EVal)
      EModify x -> EModify $ go x
      ECoModify x -> ECoModify $ go x
      l :+++: r -> go l :+++: go r
      l :|||: r -> EVal (evalEArr $ go l)
        :|||: EVal (evalEArr $ go r)
      l :&&&: r -> go l :&&&: go r
      l :***: r -> go l :***: go r
      EArr x -> EArr x
      a :>>>: b -> optEArr' $ a :>>>: b -- already did the best we could in foldcomp
      _ -> error $ "oops" ++ ashow (summarizeEArr 3 ast)

data CompSeq c a b where
  CSUnit :: c a b -> CompSeq c a b
  CSComp :: c a x -> CompSeq c x b -> CompSeq c a b
combCompSeq :: CompSeq c a b -> CompSeq c b c' -> CompSeq c a c'
combCompSeq (CSComp ax xb) bc = CSComp ax $ combCompSeq xb bc
combCompSeq (CSUnit ab) bc    = CSComp ab bc
appendCompSeq :: CompSeq (EArr c) a x -> EArr c x b -> CompSeq (EArr c) a b
appendCompSeq cs a = case cs of
  CSComp x xs -> CSComp x $ appendCompSeq xs a
  CSUnit x    -> CSComp x (CSUnit a)
asCompSeq :: EArr c a b -> CompSeq (EArr c) a b
asCompSeq ab = case ab of
  ax :>>>: xb -> asCompSeq ax `combCompSeq`  asCompSeq xb
  x           -> CSUnit x

foldrCompSeq
  :: forall c a b .
  C.Category c
  => Int
  -> (forall x y z . c x y -> c y z -> c x z)
  -> CompSeq c a b
  -> c a b
foldrCompSeq batch f = go batch where
  go :: Int -> CompSeq c x y -> c x y
  go lim = \case
    CSUnit x    -> x
    CSComp x xs -> if lim <= 0 then x >>> go batch xs else f x $ go (lim - 1) xs

compSeq :: C.Category c => CompSeq c x y -> c x y
compSeq = \case
  CSUnit x -> x
  CSComp x xs -> x >>> compSeq xs


foldWindowCompSeq
  :: forall c a b .
  C.Category c
  => Int
  -> (forall x y z . c x y -> c y z -> Maybe (c x z))
  -> CompSeq c a b
  -> CompSeq c a b
foldWindowCompSeq batch f = go batch
  where
    go :: Int -> CompSeq c x y -> CompSeq c x y
    go lim = \case
      x@(CSUnit _) -> x
      a@(CSComp x (CSUnit y)) -> maybe a CSUnit $ f x y
      CSComp x xs@(CSComp y ys) -> if lim <= 0
        then CSComp x $ go batch xs
        else maybe
          (CSComp x $ go batch xs)
          (\x' -> go (lim - 1) $ CSComp x' ys)
          $ f x y

optCons
  :: ATL c
  => (forall l m n . EArr c l m -> EArr c m n -> EArr c l n)
  -> EArr c a x
  -> EArr c x b
  -> EArr c a b
optCons cons astl astr = case (astl,astr) of
  -- Id
  (x,EId) -> x
  (EId,x) -> x
  -- Merge arr
  (EArr x,EArr y) -> EArr $ x >>> y
  (EArr x,EFirst (EArr y)) -> EArr $ x >>> first y
  (EArr x,ESecond (EArr y)) -> EArr $ x >>> second y
  (ESecond (EArr x),EArr y) -> EArr $ second x >>> y
  (EFirst (EArr x),EArr y) -> EArr $ first x >>> y
  (EArr x :***: EArr x',EArr y) -> EArr $ (x *** x') >>> y
  (EArr x,EArr y :***: EArr y') -> EArr $ x >>> (y *** y')
  (EArr x :&&&: EArr x',EArr y) -> EArr $ (x &&& x') >>> y
  (EArr x,EArr y :&&&: EArr y') -> EArr $ x >>> (y &&& y')
  -- Arrow general
  (EFirst x,EFirst y) -> EFirst $ cons x y
  (ESecond x,ESecond y) -> ESecond $ cons x y
  (l :&&&: r,EFirst y) -> cons l y :&&&: r
  (l :***: r,EFirst y) -> cons l y :***: r
  (l :&&&: r,ESecond y) -> l :&&&: cons r y
  (l :***: r,ESecond y) -> l :***: cons r y
  (EFirst y,l :***: r) -> cons y l :***: r
  (ESecond y,l :***: r) -> l :***: cons y r
  (ESecond y,EFirst x) -> x :***: y
  (EFirst x,ESecond y) -> x :***: y
  -- Modify. Note that we run `go` here because it will push
  -- down arrow operations.
  (l,r) -> case modifySeq l r of
    Nothing       -> l :>>>: r
    Just (Dict,x) -> EModify x

rewriteArr1
  :: forall c a b .
  (ATL c)
  => (forall x y . EArr c x y -> EArr c x y)
  -> EArr c a b
  -> EArr c a b
rewriteArr1 go = \case
  ELeft x -> ELeft $ go x
  ERight x -> ERight $ go x
  EFirst x -> EFirst $ go x
  ESecond x -> ESecond $ go x
  l :>>>: r -> (:>>>:) (go l) (go r)
  l :+++: r -> (:+++:) (go l) (go r)
  l :|||: r -> (:|||:) (go l) (go r)
  l :&&&: r -> (:&&&:) (go l) (go r)
  l :***: r -> (:***:) (go l) (go r)
  x -> x

rewriteArr1'
  :: forall c a b .
  ATL c
  => (forall c' x y . ATL c' => EArr c' x y -> EArr c' x y)
  -> EArr c a b
  -> EArr c a b
rewriteArr1' go = \case
  ETelescope x -> ETelescope $ go x
  ECoModify x -> ECoModify $ go x
  EUnTelescope x -> EUnTelescope $ go x
  EModify x -> EModify $ go x
  x -> rewriteArr1 go x


instance (ATL c,ArrowApply c) => ArrowApply (EArr c) where
  app = EApp
instance (ATL c,ATL (AStateArr c),Arrow (AStateArr c),ArrowState c)
  => ArrowState (EArr c) where
  type AStateSt (EArr c) = AStateSt c
  type AStateArr (EArr c) = EArr (AStateArr c)
  arrModify = EModify
  arrCoModify = ECoModify
instance (ATL c,ATL (AMachineArr c),Arrow (AMachineArr c),ArrowMachine c)
  => ArrowMachine (EArr c) where
  type AMachineArr (EArr c) = EArr (AMachineArr c)
  type AMachineNxtArr (EArr c) = EArr (AMachineNxtArr c)
  telescope = ETelescope
  untelescope = EUnTelescope


--- TESTING



printArr ast0 = do
  putStrLn "Unoptimized:"
  putStrLn $ ashow $ summarizeEArr 20 go
  putStrLn "Optimized:"
  putStrLn $ ashow $ summarizeEArr 20 (optEArr go)
  where
    go :: EArr (StateArrow (RefMap () Bool) (->)) (NodeRef ()) Bool
    go = optEArr ast0

ast :: EArr (StateArrow (RefMap () Bool) (->)) (NodeRef ()) Bool
ast =
  arr (\r -> (r,r))
  >>> first (arrModify $ arr id)
  >>> arr Left
  >>> (arr (const True) ||| ast)

-- zip with bin tree
summarizeEArr :: ATL c => Int -> EArr c a b -> EArr c a b
summarizeEArr depth =
  if depth <= 0
    then EVal . evalEArr
    else rewriteArr1 $ summarizeEArr $ depth - 1



testFullArrOpt :: Bool
testFullArrOpt = (state' $ mealy' $ select' $ evalEArr $ optEArr testArr) 0
  where
    state' c = proc x -> runStateArrow c >>> arr snd -< (mempty,x)
    mealy' c = runMealyArrow c >>> arr snd
    select' c = runSelectArrow c Cat.id
testFullArrNoOpt :: Bool
testFullArrNoOpt = (state' $ mealy' $ testArr) 0
  where
    state' c = proc x -> runStateArrow c >>> arr snd -< (mempty,x)
    mealy' c = runMealyArrow c >>> arr snd
    select' c = runSelectArrow c Cat.id

testArr :: (ArrowState c,AStateSt c ~ RefMap n Bool,ArrowChoice c)
        => c (NodeRef n) Bool
testArr = proc ref -> do
  cval <- arrModify (arr $ \(x,ref) -> (x,refLU ref x)) -< ref
  case cval of
    Just x -> returnA -< x
    Nothing -> do
      res <- comb (True,id,(&&)) (False,not,(||)) testArr -< (ref,getNs ref)
      arrInsert -< (ref,res)
{-# INLINE testArr #-}


arrInsert :: (ArrowState c,AStateSt c ~ RefMap n a) => c (NodeRef n,a) a
arrInsert = arrModify (arr $ \(x,(ref,res)) -> (refInsert ref res x,res))

comb
  :: forall c n a .
  (ArrowChoice c,ArrowState c,AStateSt c ~ RefMap n a)
  => (a
     ,a
        -> Bool
     ,a
        -> a
        -> a)
  -> (a
     ,a
        -> Bool
     ,a
        -> a
        -> a)
  -> c (NodeRef n) a
  -> c (NodeRef n,[[NodeRef n]]) a
comb (defaultAnd,isZAnd,and') (defaultOr,isZOr,or') reflu = proc (ref,refs) -> do
  false <- arrInsert -< (ref,defaultAnd)
  combOr -< const refs false
  where
    combOr :: c [[NodeRef n]] a
    combOr = proc refs -> case refs of
      [] -> returnA -< defaultOr
      rs:rss -> do
        res <- combAnd -< rs
        if isZOr res
          then returnA -< res
          else do
            ret <- combOr -< rss
            returnA -< or' res ret
    combAnd :: c [NodeRef n] a
    combAnd = proc refs -> case refs of
      [] -> returnA -< defaultAnd
      r:rs -> do
        res <- reflu -< r
        if isZAnd res
          then returnA -< res
          else do
            ret <- combAnd -< rs
            arrInsert -< (r,and' res ret)
{-# INLINE comb #-}


#endif
