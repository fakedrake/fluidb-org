{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ImplicitParams      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}
module Data.Query.Optimizations.Annotations
  ( annotateQueryPlan) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Except
import           Data.Bifunctor
import           Data.CppAst.CppType
import           Data.Either
import           Data.Function
import           Data.List
import           Data.Maybe
import           Data.Query.Algebra
import           Data.Query.QuerySchema
import           Data.Query.QuerySchema.GetQueryPlan
import           Data.Utils.AShow
import           Data.Utils.Functors
import           Data.Utils.Hashable
import           Data.Utils.Unsafe

-- | We use unwrapped e symbols because PlanSym contains information
-- about the position of a symbol in the query and we are about to
-- move them all around.
annotateQueryPlan
  :: (MonadError err m,AShowError e s err,Hashables2 e s)
  => (e -> Maybe CppType)
  -> Query (e,uid) (s,QueryPlan e s)
  -> m (Query ((e,uid),(Maybe s,CppType)) (s,QueryPlan e s))
annotateQueryPlan litType =
  fmap (first (\((x,y),z) -> (x, (fmap fst z,y)))
        . annotateTables
        (\((e,_),_) (_,qp) ->
           isJust $ lookup e (first planSymOrig <$> qpSchema qp)))
  . annotateTypes
  ((==) `on` fst)
  (\(e,_) (_,qp) ->
     columnPropsCppType <$> lookup e (first planSymOrig <$> qpSchema qp))
  (litType . fst)

-- | Annotate e with types
annotateTypes
  :: forall e' s s' e uid err m .
  (e' ~ (e,uid),MonadError err m,AShowError e s' err,Hashables2 e s)
  => (e' -> e' -> Bool)
  -> (e' -> s -> Maybe CppType)
  -> (e' -> Maybe CppType)
  -> Query e' s
  -> m (Query (e',CppType) s)
annotateTypes eqE colType litType = recur
  where
    recur :: Query e' s -> m (Query (e',CppType) s)
    recur = \case
      Q0 s -> return $ Q0 s
      Q1 o l -> do
        l' <- recur l
        let ?q = [l'] in case o of
          QProj p -> do
            p' <- forM p $ \(e,expr) -> do
              eT <- traverse sT expr
              return ((e,expT eT),eT)
            Q1 (QProj p') <$> recur l
          QGroup p es -> do
            p' <- forM p $ \(e,agg) -> do
              aT <- traverse3 sT agg
              return ((e,aggT aT),aT)
            es' <- traverse2 sT es
            Q1 (QGroup p' es') <$> recur l
          _ -> Q1 <$> traverse sT o <*> recur l
      Q2 o l r -> do
        l' <- recur l
        r' <- recur r
        o' <- let ?q = [l',r'] in traverse sT o
        return $ Q2 o' l' r'
    aggT :: Expr (Aggr (Expr (a,CppType))) -> CppType
    aggT = expT . fmap (((),) . aggT0) . fmap2 expT
      where
        aggT0 (NAggr o x) = case o of
          AggrSum   -> x
          AggrCount -> CppNat
          AggrAvg   -> x
          AggrMin   -> x
          AggrMax   -> x
          AggrFirst -> x
    expT :: Expr (a,CppType) -> CppType
    expT = fromJustErr . exprCppType' . fmap snd
    sT :: (?q :: [Query (e',CppType) s]) => e' -> m (e',CppType)
    sT e = fromJust' $ litType' e <|> listToMaybe (mapMaybe go qSchema)
      where
        qSchema = ?q >>= querySchemaNaive
        fromJust' =
          maybe
            (throwAStr
             $ "Failed to deduce symbol type."
             ++ ashow (fst e,redactSch qSchema))
            return
        redactSch =
          fmap
          $ bimap (const ())
          $ bimap
            (bimap (fmap (bimap (first fst) $ fmap $ first fst)) $ const ())
          $ bimap (first fst) (const ())
        litType' e' = (e',) <$> litType e'
        go :: Either s (Either [((e',CppType),x)] ([((e',CppType),y)],z),w)
           -> Maybe (e',CppType)
        go = \case
          Left s                -> (e,) <$> colType e s
          Right (Left p,_)      -> luE $ fst <$> p
          Right (Right (p,_),_) -> luE $ fst <$> p
        luE :: [(e',CppType)] -> Maybe (e',CppType)
        luE = find (eqE e . fst)

annotateTables
  :: forall e' s . (e' -> s -> Bool) -> Query e' s -> Query (e',Maybe s) s
annotateTables isInCol = \case
  Q2 o l r -> let l' = recur l
                  r' = recur r
                  syms = lefts $ [l',r'] >>= querySchemaNaive
    in Q2 ((`f` syms) <$> o) l' r'
  Q1 o l -> let l' = recur l
                syms = lefts $ querySchemaNaive l' in case o of
    QGroup p es -> Q1
      (QGroup
         [((e,Nothing),fmap3 (`f` syms) expr) | (e,expr) <- p]
         (fmap2 (`f` syms) es))
      l'
    QProj p -> Q1
      (QProj [((e,Nothing),(`f` syms) <$> expr) | (e,expr) <- p])
      l'
    _ -> Q1 ((`f` syms) <$> o) l'
  Q0 s -> Q0 s
  where
    f :: e' -> [s] -> (e',Maybe s)
    f e ss = (e,listToMaybe $ filter (isInCol e) ss)
    recur = annotateTables isInCol
