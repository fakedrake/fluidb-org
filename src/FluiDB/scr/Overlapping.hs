{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

module Database.FluiDB.Running.Overlapping
  ( eqJoinQuery
  , joinKeyOverlap
  , skipSubsuming
  ) where

import           Database.FluiDB.Codegen.SchemaAssocClass
import           Database.FluiDB.Poset
import           Database.FluiDB.Utils
import           Unsafe.Coerce

import           Control.Monad.Writer
import           Data.List
import           Data.Maybe
import           Database.FluiDB.Algebra
import           Database.FluiDB.Codegen.Build.Monads
import           Database.FluiDB.Optimizations.TightenJoins

symAssoc :: SchemaAssoc e s -> [(e, s)]
symAssoc schemaAssoc = do
  (dat, sch) <- schemaAssoc
  (_, sym) <- sch
  return (sym, dat)

-- |From equality pairs make a query of joins for the tpc-h schema.
eqJoinQuery :: forall e s . (Eq s, Eq e) =>
              SchemaAssoc e s -> [(e,e)] -> Query e s
eqJoinQuery schemaAssoc eqs = case P0 . toRel <$> filter (uncurry (/=)) eqs of
  []   -> prod
  p:ps -> S (foldl And p ps) prod
  where
    eqSyms :: [e]
    eqSyms = eqs >>= (\(x,y) -> [x,y])
    xs = nub [fromJust' s $ s `lookup` symAssoc schemaAssoc | s <- eqSyms]
    prod = foldl1 (Q2 QProd) $ Q0 <$> xs
    toRel (e, e') = R2 REq (R0 (E0 e)) (R0 (E0 e'))
    fromJust' s = \case
      Nothing -> error $ "Oops: "
        ++ show (unsafeCoerce s :: (FLat Integer, FLat Integer))
        ++ "\nin\n"
        ++ show (unsafeCoerce $ symAssoc schemaAssoc :: [((FLat Integer, FLat Integer), FLat Integer)])
        ++ "\nie\n"
        ++ show (unsafeCoerce schemaAssoc :: SchemaAssoc (FLat Integer, FLat Integer) (FLat Integer))
      Just x -> x

when' :: (Monad m, Monoid a) => Bool -> m a -> m a
when' p x = if p then x else return mempty

-- |get clusters of queries that have more than thresh symbols.
joinKeyOverlap :: forall e s t n m .
                 (MonadCodeError e s t n m,
                  MonadCodeBuilder e s t n m,
                  Eq e) =>
                 Int -> Int -> [(FilePath, Query e s)]
               -> m [([(e,e)], [(FilePath, Query e s)])]
joinKeyOverlap _ _ [] = return []
joinKeyOverlap minEqs minSize ((fp,q):qs) = when' (minSize - 1 <= length qs) $ do
  fromSym <- toSymbol . cbQueryCppConf <$> getCBState
  let eqs :: [(e,e)] = nub $ catMaybes $ cleanEq (fmap afterDot . fromSym)
        <$> extractEqsQ q
  let justQM = when' (minSize <= 1) $ return [(eqs, [(fp,q)])]
  let withQM = when' (length eqs >= minEqs) $ do
        rest <- joinKeyOverlap minEqs (minSize - 1) qs
        return $ do
          (eqs', qs') <- rest
          let inters = eqs `intersect` eqs'
          guard $ length inters > minEqs
          [(inters, (fp,q):qs')]
  let withoutQM = joinKeyOverlap minEqs minSize qs
  join <$> sequenceA [justQM, withQM, withoutQM]

skipSubsuming :: Eq a => (b -> [a]) -> [b] -> [b]
skipSubsuming _ [] = []
skipSubsuming _ [x] = [x]
skipSubsuming toL (a:rest) = go a [] rest
  where
    go x cleanRest [] = x:skipSubsuming toL cleanRest
    go x cleanRest (x':dirtyRest) = case f x x' of
      (newX, Just newX') -> go newX (newX':cleanRest) dirtyRest
      (newX, Nothing)    -> go newX cleanRest dirtyRest
    f p p' = if union as as' `longerThan` maxLen
             then (p, Just p')
             else (maxPair, Nothing)
      where
        as = toL p
        as' = toL p'
        (maxLen, maxPair) = if len < len' then (len', p') else (len, p)
          where
            len = length as
            len' = length as'

cleanEq :: Ord a => (e -> Maybe a) -> (e, e) -> Maybe (e, e)
cleanEq fromSym (x, y) = do
  x' <- fromSym x
  y' <- fromSym y
  return $ if x' < y' then (x, y) else (y, x)

afterDot :: String -> String
afterDot = reverse . go [] where
  go res = \case
    [] -> res
    '.':xs -> go [] xs
    x:xs -> go (x:res) xs
