{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Query.Optimizations.PushSelections
  ( pushSelections
  , pushSelections'
  , couldPushIn
  ) where

import           Data.Functor.Identity
import           Data.List
import           Data.Query.Algebra
import           Data.Query.Optimizations.Types
import           Data.Utils.Function
import           Data.Utils.Functors


-- | Decide if all the symbols of a prop make sense entirely within a
-- certain query.
couldPushIn :: forall e' e s .
              SymEmbedding e s e'
            -> Prop (Rel (Expr e'))
            -> Query e' s
            -> Bool
couldPushIn SymEmbedding{..} prop query =
  all (\s -> embedIsLit s || refersToPlan query s) $ toList3 prop
  where
    refersToPlan :: Query e' s -> e' -> Bool
    refersToPlan q e = (`any` querySchemaNaive q) $ \case
      Left s -> e `embedInS` s
      Right (Left p,_) -> elem' e $ fst <$> p
      Right (Right (p,_),_) -> elem' e $ fst <$> p
      where
        elem' :: e' -> [e'] -> Bool
        elem' = any . symEq

pushSelections :: forall e e' s .
                 SymEmbedding e s e'
               -> Query e' s
               -> Query e' s
pushSelections symEmb =
  pushSelections' (symEq symEmb) $ \p q -> couldPushIn symEmb p q

pushSelections' :: forall e s .
                  (e -> e -> Bool)
                -> (Prop (Rel (Expr e)) -> Query e s -> Bool)
                -> Query e s
                -> Query e s
pushSelections' eqE couldPushIn0 = freshPush
  where
    freshPush = fst . push []
    unQnf :: [Prop (Rel (Expr e))] -> Prop (Rel (Expr e))
    unQnf = foldl1 And
    -- Push and keep around the leftovers
    qnfAnd' p = fmap4 unWrapEq $ toList $ propQnfAnd $ fmap3 (WrapEq eqE) p
    push :: [Prop (Rel (Expr e))]
         -> Query e s
         -> (Query e s,[Prop (Rel (Expr e))])
    push r = \case
      S p q -> push (qnfAnd' p ++ r) q
      J p q q' -> push (qnfAnd' p ++ r) (Q2 QProd q q')
      q@(Q2 QProd q0 q1)
        -> let (r',rleftover) = partition (`couldPushIn0` q) r
               (q0',r'') = push r' q0
               (q1',rjoin) = push r'' q1
               maybeJoin = makeJoin rjoin
           in (q0' `maybeJoin` q1',rleftover)
      Q1 (QSort s) q -> let (q',r') = push r q
                        in (Q1 (QSort s) q',r')
      q -> putOrPush r q
    putOrPush :: [Prop (Rel (Expr e))]
              -> Query e s
              -> (Query e s,[Prop (Rel (Expr e))])
    putOrPush r q =
      let (rq,rq') = partition (`couldPushIn0` q) r
          q' = runIdentity $ qmapA1 (Identity . freshPush) q
      in if null rq
           then (q',r)
           else (S (unQnf rq) q',rq')
    -- Decides whether we should be doing
    -- * A product
    -- * A join
    -- * A join (equijoin) and then a selection
    --
    -- XXX: queries 12, 17, 22, 21, 20 would benefit from merge joining
    -- even though they are not equijoins.
    --
    -- XXX: Here we ignore if the equaliy is well behaved.
    makeJoin :: [Prop (Rel (Expr e))] -> Query e s -> Query e s -> Query e s
    makeJoin qnf =
      let isEqualityTerm = \case
            P0 (R2 REq _ _) -> True
            _ -> False
      in case partition isEqualityTerm qnf of
           ([],[]) -> Q2 QProd
           ([],terms) -> J $ unQnf terms
           (terms,[]) -> J $ unQnf terms
           (eqTerms,neqTerms) -> S (unQnf neqTerms)
             ... J (unQnf eqTerms)
