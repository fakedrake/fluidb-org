{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

module Data.Query.Optimizations.TightenJoins
  ( possibleJoins
  , extractEqsQ
  , extractEqs
  , cleanLiteralEqs
  , demoteTightenErr
  ) where

import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer
import           Data.Bifunctor
import qualified Data.Constraint                as DC
import           Data.List
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Query.Optimizations.Types
import           Data.Query.QuerySchema
import           Data.Tuple
import           Data.Utils.AShow
import           Data.Utils.Functors
import           GHC.Generics

demoteTightenErr :: TightenErr (ShapeSym e s) (QueryShape e s,s) -> TightenErr e s
demoteTightenErr (TightenErrMsg x) = TightenErrMsg $ demoteAStr (DC.Sub DC.Dict) x

type MonadTighten e s m = (MonadReader (TightenConf e s) m,
                           MonadError (TightenErr e s) m)

isInQuery :: forall e s m . MonadTighten e s m => e -> Query e s -> m Bool
isInQuery e q = do
  isInSym <- asks tightenConfIsInSym
  symEq <- asks tightenConfESymEq
  let elem' :: e -> [e] -> Bool = any . symEq
  -- Soft element equality
  return $ foldSchema
    (&&)
    (elem' e . either (fmap fst) (fmap fst))
    (isInSym e <$> q :: Query e Bool)

-- | The selection can apply to ANY OF the queries. NOT ALL OF (by way
-- of product).
data AllJoinsQuery e s
  = AllJoinsQuery (Maybe (Prop (Rel (Expr e)))) [Query e (AllJoinsQuery e s)]
  | AllJoinsQueryPure s
  deriving (Show,Eq,Generic)
instance AShow2 e s => AShow (AllJoinsQuery e s)
instance ARead2 e s => ARead (AllJoinsQuery e s)

-- |Get all possible join patterns.
possibleJoins :: MonadTighten e s m => Query e s -> m [Query e s]
possibleJoins = fmap joinsToQuery . possibleJoinsInternal

joinsToQuery :: AllJoinsQuery e s -> [Query e s]
joinsToQuery = \case
  AllJoinsQuery pM qs -> do
    q <- qs
    mergeSel <$> case pM of
      Just p  -> [S p $ join q0 | q0 <- traverse joinsToQuery q]
      Nothing -> join <$> traverse joinsToQuery q
  AllJoinsQueryPure s -> [Q0 s]
  where
    mergeSel = \case
      S p (S p' q) -> mergeSel $ S (And p p') q
      q            -> runIdentity $ qmapA1' (return . Q0) (return . mergeSel) q

-- | All possible products (just one for now, we don't like products)
allProducts :: [Query e s] -> [Query e s]
allProducts [] = []
allProducts xs = return $ foldl1 (Q2 QProd) xs

-- | From a query find a set of possible join patterns that will be
-- inserted in the graph.
--
-- XXX: Get the nodes from queries
possibleJoinsInternal
  :: forall e s m . MonadTighten e s m => Query e s -> m (AllJoinsQuery e s)
possibleJoinsInternal query = do
  isLit <- asks tightenConfIsLit
  go isLit query
  where
    go :: (e -> Bool) -> Query e s -> m (AllJoinsQuery e s)
    go isLit = \case
      Q0 s -> return $ AllJoinsQueryPure s
      q -> case topLevelSelection q of
        (Nothing,qs) -> fmap (AllJoinsQuery Nothing . allProducts)
          $ qmapA1' (return . Q0 . AllJoinsQueryPure) (fmap Q0 . go isLit)
          `traverse` qs
        (Just prop,qs) -> do
          let (propMNoEq,eqsSimpleDirty) = extractEqs prop
          -- Get the equalities between symbols (not literals).
          let eqsSimple = cleanLiteralEqs isLit eqsSimpleDirty
          qAssoc
            :: [(Query e s,Query e (AllJoinsQuery e s))] <- forM qs $ \q0 -> do
            q' <- (fmap Q0 . go isLit) q0
            return (q0,q')
          eqsAnnotated :: [((e,Query e (AllJoinsQuery e s))
                           ,(e,Query e (AllJoinsQuery e s)))]
            <- annotateEqs qAssoc eqsSimple
          eqE <- asks tightenConfESymEq
          eqS <- asks tightenConfSSymEq
          let qs' :: [FreeEqJoin e (Query e (AllJoinsQuery e s))] =
                allEqJoins (eqQJoins eqE eqS) (Q2 QProd) eqsAnnotated
                $ snd <$> qAssoc
          return $ AllJoinsQuery propMNoEq $ join . eqJoinToQuery <$> qs'

eqQJoins :: (e -> e -> Bool) -> (s -> s -> Bool)
         -> Query e (AllJoinsQuery e s)
         -> Query e (AllJoinsQuery e s)
         -> Bool
eqQJoins eqE eqS q q' =
  bimap (const ()) (const ()) q == bimap (const ()) (const ()) q'
  && and (zipWith eqE (toList $ FlipQuery q) (toList $ FlipQuery q'))
  && and (zipWith eqJoins (toList q) (toList q'))
  where
    recur = eqQJoins eqE eqS
    eqJoins = curry $ \case
      (AllJoinsQuery prop qs,AllJoinsQuery prop' qs') -> fmap3 void prop
        == fmap3 void prop'
        && and (zipWith eqE (toList4 prop) (toList4 prop'))
        && setEq recur qs qs'
      (AllJoinsQueryPure s,AllJoinsQueryPure s') -> eqS s s'
      _ -> False
    setEq _ [] [] = True
    setEq eq (x:xs) ys =
      any (eq x) ys
      && setEq eq (filter (not . eq x) xs) (filter (not . eq x) ys)
    setEq _ _ _ = False


chooseQ :: MonadTighten e s m => [(Query e s, q)] -> e -> m q
chooseQ qs e = do
  -- (\(q,q') -> fmap2 (const q') $ _exprSymType e q)
  qsM <- forM qs $ \(v,r) ->
    isInQuery e v <&> \case {True -> Just (v,r); False -> Nothing}
  case catMaybes qsM of
    [(_,q)] -> return q
    []   -> throwAStr $ "Free variable found: " ++ ashow e ++  " not in " ++ ashow  (fst <$> qs)
    qs'   -> throwAStr $ "Ambiguous variable found: "
            ++ ashow e ++  " in more than one: " ++ ashow  (fst <$> qs')


-- | Get all equalities between symbols (we assume that is between
-- non-literals)
cleanLiteralEqs :: (e -> Bool) -> [(e, e)] -> [(e, e)]
cleanLiteralEqs isLit = filter (\(e,e') -> not $ isLit e || isLit e')

extractEqs :: forall e . Prop (Rel (Expr e)) -> (Maybe (Prop (Rel (Expr e))), [(e, e)])
extractEqs = runWriter . go where
  go :: Prop (Rel (Expr e)) -> Writer [(e,e)] (Maybe (Prop (Rel (Expr e))))
  go = \case
    P0 (R2 REq (R0 (E0 l)) (R0 (E0 r))) -> tell [(l,r)] >> return Nothing
    And x y -> ((,) <$> go x <*> go y) <&> (\case
      (Nothing,Nothing) -> Nothing
      (Just l,Nothing)  -> Just l
      (Nothing,Just r)  -> Just r
      (Just l,Just r)   -> Just $ And l r)
    x -> return $ Just x

extractEqsQ :: Query e s -> [(e, e)]
extractEqsQ = execWriter . eqsQ
  where
    eqsQ = qmapM $ \case
      J p q q' -> eqsP p >> return (J p q q')
      S p q    -> eqsP p >> return  (S p q)
      q        -> return q
    eqsP :: Prop (Rel (Expr e)) -> Writer [(e,e)] ()
    eqsP = tell . snd . extractEqs

-- |if there is no top level selection this is equivalent to
-- \x -> (Nothing, [x])
--
-- > topLevelSelection
--     $ J (P0 (R2 REq (R0 (E0 'a')) (R0 (E0 'a')))) (Q0 "a") (Q0 "b")
-- (Just (P0 (R2 REq (R0 (E0 'a')) (R0 (E0 'a')))),[Q0 "a",Q0 "b"])
topLevelSelection :: Query e s -> (Maybe (Prop (Rel (Expr e))), [Query e s])
topLevelSelection =
  swap . second unMaybe . (`runState` (P0 $ R0 $ E0 Nothing)) . go
  where
    unMaybe :: Prop (Rel (Expr (Maybe e))) -> Maybe (Prop (Rel (Expr e)))
    unMaybe = \case
      And (P0 x) (P0 y) -> case (traverse2 id x,traverse2 id y) of
        (Just r,Just r')  -> Just $ And (P0 r) (P0 r')
        (Just r,Nothing)  -> Just $ P0 r
        (Nothing,Just r)  -> Just $ P0 r
        (Nothing,Nothing) -> Nothing
      And (P0 x) y -> case traverse2 id x of
        Just r  -> And (P0 r) <$> unMaybe y
        Nothing -> unMaybe y
      And y (P0 x) -> case traverse2 id x of
        Just r  -> (`And` P0 r) <$> unMaybe y
        Nothing -> unMaybe y
      p -> qmapA1' (fmap P0 . traverse2 id) unMaybe p
    -- | Gatger all selection propositions into the state
    go :: Query e s -> State (Prop (Rel (Expr (Maybe e)))) [Query e s]
    go = \case
      J p l r      -> go $ S p $ Q2 QProd l r
      S p q        -> modify (And $ fmap3 Just p) >> go q
      Q2 QProd l r -> (<>) <$> go l <*> go r
      q            -> return [q]

data FreeEqJoin e s
  = FreeEqJoin [(e,e)] (FreeEqJoin e s) (FreeEqJoin e s)
  | PureEqJoin s
eqJoinToQuery :: FreeEqJoin e s -> Query e s
eqJoinToQuery = \case
  PureEqJoin s -> Q0 s
  FreeEqJoin [] _ _ -> undefined
  FreeEqJoin (e:es) l r ->
    J (foldl andP (toEq e) es) (eqJoinToQuery l) (eqJoinToQuery r)
    where
      andP x p = And (toEq p) x
      toEq (el, er) = P0 $ R2 REq (R0 $ E0 el) (R0 $ E0 er)


-- | Annotate each symbol of an equation with the corresponding
-- subquery.
annotateEqs :: MonadTighten e s m =>
              [(Query e s, q')]
            -> [(e, e)]
            -> m [((e, q'), (e, q'))]
annotateEqs qs = mapM $ \(e1, e2) -> do
  q1 <- chooseQ qs e1
  q2 <- chooseQ qs e2
  return ((e1, q1), (e2, q2))

-- | The actual work of finding all the pairs. Note that the pairs we
-- extract need to be connected.
allEqJoins
  :: forall q e s .
  (q ~ Query e s)
  => (q -> q -> Bool)
  -> (q -> q -> q)
  -> [((e,q),(e,q))]
  -> [q]
  -> [FreeEqJoin e q]
allEqJoins _ _ _ [] = []
allEqJoins _ _ [] [q] = [PureEqJoin q]
allEqJoins _ _ es [q] = [PureEqJoin $ S (foldl1 And eqs) q] where
  eqs = [P0 (R2 REq (atom2 e) (atom2 e')) | ((e,_),(e',_)) <- es]
allEqJoins eqQ prodFn edgesES as = do
  let edgesS = bimap snd snd <$> edgesES
  (s1, s2) <- possibleSplits as --
  guard $ notPartitioned eqQ edgesS s1 && notPartitioned eqQ edgesS s2
  let elem' e = not . any (eqQ e)
  let internalTo s ((_,t),(_,t')) = t `elem'` s && t' `elem'` s
  let (e1, edgesES') = partition (internalTo s1) edgesES
  let (e2, edgesES'') = partition (internalTo s2) edgesES'
  -- If edgesES'' == () we are creating a product. Move hte product to the end
  case edgesES'' of
    _:_ -> zipWithLongest
      (FreeEqJoin $ bimap fst fst <$> edgesES'')
      (allEqJoins eqQ prodFn e1 s1) -- maybe keep some of of these into edgesES''. Edges
                                    -- are hot or cold, try keeping
                                    -- the cold ones in edgesES'' to
                                    -- produce hot subexpressions.
      (allEqJoins eqQ prodFn e2 s2)
    [] -> do
      -- There is no edgeES connecting between e1 and e2. Try all
      -- products for arbitrary connection. All these products must be
      -- at the bottom.
      --
      (prodl, prodr) <- (,) <$> s1 <*> s2
      let productQ = prodFn prodl prodr
      let repl = [(prodl,productQ),(prodr,productQ)]
      allEqJoins eqQ prodFn
        (replaceQBoth eqQ repl <$> edgesES)
        (nub' $ replaceQId eqQ repl <$> as)
  where
    nub' []     = []
    nub' (x:xs) = x:nub' (filter (not . eqQ x) xs)
    zipWithLongest f l r = zipWith f l' r' where
      (l', r') = if length l < length r then (pad l, r) else (l, pad r)
      pad = \case {[] -> undefined; [x] -> repeat x; x:xs -> x:pad xs}

replaceQId :: (Foldable t) => (b -> b -> Bool) -> t (b, b) -> b -> b
replaceQId eq rs q = foldl replaceQ1 q rs where
  replaceQ1 q' (f,t) = if f `eq` q' then t else q'
replaceQSec :: (Bifunctor p, Foldable t) =>
              (c -> c -> Bool) -> t (c, c) -> p a c -> p a c
replaceQSec eq r = second $ replaceQId eq r
replaceQBoth
  :: (Foldable t,Bifunctor p1,Bifunctor p2,Bifunctor p3)
  => (c -> c -> Bool)
  -> t (c,c)
  -> p1 (p2 a1 c) (p3 a2 c)
  -> p1 (p2 a1 c) (p3 a2 c)
replaceQBoth eq r = replaceQSec eq r `bimap` replaceQSec eq r


-- |Split all possible it shouldn't matter.
possibleSplits :: [s] -> [([s],[s])]
-- possibleSplits [] = [([], [])]  -- Unreachable
-- possibleSplits [x] = [([x], [])] -- unreachable
possibleSplits [x,y] = [([x],[y])]
possibleSplits (x:xs) = (first (x:) <$> rest) ++ (second (x:) <$> rest) where
  rest = possibleSplits xs
possibleSplits _ = error "Should be unreachable, we dnont want an empty side"

notPartitioned :: forall s . (s -> s -> Bool) -> [(s, s)] -> [s] -> Bool
notPartitioned _ _ [] = True
notPartitioned eq edges (i:is) = go [i] is where
  go tips = \case
    [] -> True
    maybeConn -> case conn of
      [] -> False
      _  -> go conn noConn
      where
        (conn,noConn) = partition (\cand -> connectedTo cand `any` tips) maybeConn
        connectedTo a b = connects a b `any` edges where
          connects :: s -> s -> (s,s) -> Bool
          connects x' y' (x,y) = eq x x' && eq y y' || eq x y' && eq y x'
