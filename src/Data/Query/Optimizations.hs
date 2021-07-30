{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC -Wno-deferred-type-errors #-}
module Data.Query.Optimizations
  (sanitizeQuery
  ,optQuery'
  ,mkEmbedding
  ,optQueryPlan
  ,OptimizationError(..)
  ,TightenErr(..)) where

import           Control.Monad.Except
import           Control.Utils.Free
import           Data.Bifunctor
import           Data.CppAst.CppType
import qualified Data.List.NonEmpty                    as NEL
import           Data.Maybe
import           Data.QnfQuery.Types
import           Data.Query.Algebra
import           Data.Query.Optimizations.Annotations
import           Data.Query.Optimizations.Dates
import           Data.Query.Optimizations.Echo
import           Data.Query.Optimizations.EchoingJoins
import           Data.Query.Optimizations.Likes
import           Data.Query.Optimizations.Misc
import           Data.Query.Optimizations.Projections
import           Data.Query.Optimizations.Sort
import           Data.Query.Optimizations.Types
import           Data.Query.Optimizations.Utils
import           Data.Query.QuerySchema
import           Data.Query.SQL.Types
import           Data.Utils.AShow
import           Data.Utils.Compose
import           Data.Utils.Hashable
import           Data.Utils.Tup

sanitizeQuery
  :: SymEmbedding (ExpTypeSym' e) s e'
  -> Query e' s
  -> Maybe (Query e' s)
sanitizeQuery symEmb =
  fmap
    (fixSubstringIndexOffset
     . optimizeSorts (symEq symEmb)
     . sortBeforeAggregating (symEq symEmb)
     . likesToEquals symEmb)
  . squashDates symEmb
  . squashProjections (symEq symEmb)

mkEmbedding
  :: Hashables2 e s
  => (e
      -> ExpTypeSym' e0
     ,ExpTypeSym' e0
      -> e)
  -> SymEmbedding
    (ExpTypeSym' e0)
    (s,QueryPlan e s)
    (PlanSym e s,(Maybe s,CppType))
mkEmbedding (toETS,toE) =
  SymEmbedding
  { embedLit =
      \e -> (mkLitPlanSym $ toE e,(Nothing,fromJust $ expTypeSymCppType e))
   ,unEmbed = toETS . planSymOrig . fst
   ,symEq = \(x,_) (y,_) -> planSymOrig x == planSymOrig y
   ,embedType = Just . snd . snd
   ,embedInS = \(_,(s0,_)) s -> Just (fst s) == s0
   ,embedIsLit =
      (\case
         NonSymbolName _ -> True
         _               -> False) . planSymQnfName . fst
  }

optQuery'
  :: forall e' e s .
  Hashables2 e' s
  => SymEmbedding (ExpTypeSym' e) s e'
  -> Query e' s
  -> Maybe (Free (Compose NEL.NonEmpty (TQuery e')) s)
optQuery'  symEmb q0 = do
  saneq <- sanitizeQuery symEmb q0
  -- Actual optimizations
  let withLikesQ :: Query e' s =
         equalsToLikes (embedIsLit symEmb) isString saneq
  joinPermutations symEmb withLikesQ
  where
    isString s = case embedType symEmb s of
      Just (CppArray CppChar _) -> True
      _                         -> False

optQueryPlan
  :: forall e s e0 err m .
  (Hashables2 e s,MonadError err m,AShowError e s err)
  => (e -> Maybe CppType)
  -> (e
      -> ExpTypeSym' e0
     ,ExpTypeSym' e0
      -> e)
  -> Query (PlanSym e s) (QueryPlan e s,s)
  -> m (Free (Compose NEL.NonEmpty (TQuery e)) (s,QueryPlan e s))
optQueryPlan litType etsIso q = do
  annotated <- annotateQueryPlan litType
    $ first (\s -> (planSymOrig s,s))
    $ fmap swap q
  optq <- maybe
    (throwAStr
     $ "optQuery' failed: " ++ ashow (bimap (fst . fst) fst annotated))
    return
    $ optQuery' (mkEmbedding etsIso)
    $ first (first snd) annotated
  return
    $ hoistFreeTF
      (Compose . fmap (first $ planSymOrig . fst) . getCompose)
      optq

hoistFreeTF
  :: (Monad m,Functor f)
  => (forall a . f a -> g a)
  -> FreeT f m b
  -> FreeT g m b
hoistFreeTF mh = go
  where
    go = FreeT . fmap (\case
                         (Pure b) -> Pure b
                         (Free f) -> Free $ mh $ go <$> f) . runFreeT
