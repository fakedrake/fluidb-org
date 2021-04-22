{-# LANGUAGE CPP                 #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies        #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
module Data.Query.Optimizations.PossibleJoins (joinPermutations) where

import           Control.Monad.Free
import           Data.Bifunctor
import           Data.Bitraversable
import           Data.List
import qualified Data.List.NonEmpty                      as NEL
import           Data.Query.Algebra
import           Data.Query.Optimizations.PushSelections
import           Data.Query.Optimizations.Types
import           Data.Utils.Compose
import           Data.Utils.Functors

-- | Make an equijoin and a selection.
selAndJoin :: NEL.NonEmpty (Prop (Rel (Expr e')))
           -> q -> q -> NEL.NonEmpty (Query e' q)
selAndJoin ps l r = case partition isEq $ toList ps of
  (eqs,[]) -> return $ J (foldl1 And eqs) (Q0 l) (Q0 r)
  ([],nonEqs) -> return $ J (foldl1 And nonEqs) (Q0 l) (Q0 r)
  (eqs,nonEqs) -> NEL.cons
    (S (foldl1 And nonEqs) $ J (foldl1 And eqs) (Q0 l) (Q0 r))
    [J (foldl1 And ps) (Q0 l) (Q0 r)]
  where
    isEq = \case {P0 (R2 REq _ _) -> True; _ -> False}

-- | The input list is just a product of all the queries. The output
-- list is the possible joins.
productPermutations :: forall e s e' s0 .
                      (s0 ~ Free (Compose NEL.NonEmpty (Query e')) s) =>
                      SymEmbedding e s e'
                    -> ([Prop (Rel (Expr e'))],NEL.NonEmpty (Query e' s0))
                    -> Maybe (Free (Compose NEL.NonEmpty (Query e')) s)
productPermutations emb (ps0,ts0) = go (ps',ts')
  where
    ps' =
      [([i | (i,t) <- toList tsQ'
            ,e <- toList3 p
            ,not $ embedIsLit emb e
            ,couldPushIn emb (return3 e) t]
       ,p) | p <- ps0]
    tsQ' = fmap2 (>>= asQ) ts'
    ts' :: NEL.NonEmpty
          (Int,Query e' (Free (Compose NEL.NonEmpty (Query e')) s))
    ts' = NEL.zip (0 NEL.:| [1 ..]) ts0
    asQ :: Free (Compose NEL.NonEmpty (Query e')) s -> Query e' s
    asQ = NEL.head . getCompose . retract
    go :: ([([Int],Prop (Rel (Expr e')))]
          ,NEL.NonEmpty
             (Int,Query e' (Free (Compose NEL.NonEmpty (Query e')) s)))
       -> Maybe (Free (Compose NEL.NonEmpty (Query e')) s)
    go = fmap (Free . Compose) . \case
      ([],(_,q') NEL.:| []) -> Just $ (NEL.:| []) q'
      (ps,(_,q') NEL.:| []) -> Just $ return $ S (foldr1 And $ snd <$> ps) q'
      ([],_ NEL.:| (_:_)) -> Nothing
      (ps,ts) -> NEL.nonEmpty $ do
        -- Partition subqs
        (l,r) <- possibleSplits ts
        -- Partition props
        (onLeft,rest) <- partitionPushable (fst <$> l) ps
        (onRight,onJListM) <- partitionPushable (fst <$> r) rest
        onJList <- maybe [] return $ NEL.nonEmpty onJListM
        toList2
          $ selAndJoin (snd <$> onJList) <$> go (onLeft,l) <*> go (onRight,r)
      where
        partitionPushable
          :: NEL.NonEmpty Int
          -> [([Int],prop)]
          -> [([([Int],prop)],[([Int],prop)])]
        partitionPushable qs ps = do
          let (pushable,nonPushable) =
                partition ((`isSubsequenceOf` toList qs) . fst) ps
          case partition ((<= 1) . length . fst) pushable of
            ([],connecting) -> return (connecting,nonPushable)
            (single,connecting)
              -> [(connecting ++ single,nonPushable)
                 ,(connecting,nonPushable ++ single)]
        -- At least one on each side
        possibleSplits :: NEL.NonEmpty a -> [(NEL.NonEmpty a,NEL.NonEmpty a)]
        possibleSplits = go'
          where
            go' (_ NEL.:| []) = undefined
            go' (a NEL.:| [b]) = [(return a,return b)]
            go' (x0 NEL.:| (x:xs)) = onSs first ++ onSs second
              where
                onSs side = fmap (side (NEL.cons x0)) $ go' $ x NEL.:| xs

toProduct1 :: Query e s -> ([Prop (Rel (Expr e))],NEL.NonEmpty (Query e s))
toProduct1 = recur where
  recur = \case
    J p l r -> recur $ S p $ Q2 QProd l r
    S p q' -> first (p:) $ recur q'
    Q2 QProd l r -> let {(lp,lq) = recur l; (rp,rq) = recur r}
                   in (lp <> rp,lq <> rq)
    q' -> ([],return q')

joinPermutations :: forall e s e' .
                   SymEmbedding e s e'
                 -> Query e' s
                 -> Maybe (Free (Compose NEL.NonEmpty (Query e')) s)
joinPermutations emb = recur . pushSelections emb where
  recur :: Query e' s -> Maybe (Free (Compose NEL.NonEmpty (Query e')) s)
  recur = (>>= productPermutations emb)
          . bitraverse cnfAnd' (traverse dropLayerAndRecur)
          . toProduct1
  cnfAnd' :: [Prop (Rel (Expr e'))] -> Maybe [Prop (Rel (Expr e'))]
  cnfAnd' = Just . fmap4 unWrapEq . (>>= toList . propCnfAnd) . fmap4 (WrapEq (symEq emb))
  dropLayerAndRecur :: Query e' s
                    -> Maybe (Query e' (Free (Compose NEL.NonEmpty (Query e')) s))
  dropLayerAndRecur = \case
    Q2 o l r -> q2 o <$> recur l <*> recur r
    Q1 o q -> q1 o <$> recur q
    Q0 s -> Just $ Q0 $ Pure s
  q2 o l r = Q2 o (Q0 l) (Q0 r)
  q1 o q = Q1 o $ Q0 q

#if 0

joinGraph :: [(Int,Int)] -> ([Prop (Rel (Expr Int))],NEL.NonEmpty (Query Int [Int]))
joinGraph jg = (eq <$> jg ,
                fromJust $ NEL.nonEmpty $ nub $ jg >>= \(x,y) -> [Q0 [x],Q0 [y]])
  where
    eq (x,y) = P0 $ R2 REq (R0 (E0 x)) (R0 (E0 y))

emb :: (Eq e, Eq l) => SymEmbedding (Either e l) [e] (Either e l)
emb = SymEmbedding {
  embedLit=id,
  unEmbed=id,
  symEq=(==),
  embedType=const $ Just CppNat,
  embedInS= \e s -> either (`elem` s) (const False) e,
  embedIsLit=isRight
  }

type SSym = String
type SLit = String
test :: Maybe [Query (Either SSym SLit) [String]]
test = fmap (toList . getCompose . retract) $ joinPermutations emb q
q :: Query (Either SSym SLit) [String]
q = (S
        (P2
          PAnd
          (P2
            PAnd
            (P2
              PAnd
              (P2
                PAnd
                (P2
                  PAnd
                  (P2
                    PAnd
                    (P2
                      PAnd
                      (P2
                        PAnd
                        (P2
                          PAnd
                          (P0
                            (R2 REq
                                (R0 (E0 (Left "p_partkey")))
                                (R0 (E0 (Left "l_partkey")))))
                          (P0
                            (R2 REq
                                (R0 (E0 (Left "s_suppkey")))
                                (R0 (E0 (Left "l_suppkey"))))))
                        (P0
                          (R2 REq
                              (R0 (E0 (Left "l_orderkey")))
                              (R0 (E0 (Left "o_orderkey"))))))
                      (P0
                        (R2 REq
                            (R0 (E0 (Left "o_custkey")))
                            (R0 (E0 (Left "c_custkey"))))))
                    (P0
                      (R2 REq
                          (R0 (E0 (Left "c_nationkey")))
                          (R0 (E0 (Left "n1.n_nationkey"))))))
                  (P0
                    (R2 REq
                        (R0 (E0 (Left "n1.n_regionkey")))
                        (R0 (E0 (Left "r_regionkey"))))))
                (P0
                  (R2 REq
                      (R0 (E0 (Left "r_name")))
                      (R0 (E0 (Right "AFRICA"))))))
              (P0
                (R2 REq
                    (R0 (E0 (Left "s_nationkey")))
                    (R0 (E0 (Left "n2.n_nationkey"))))))
            (P2
              PAnd
              (P0
                (R2
                  RLe
                  (R0
                    (E0
                      (Right "1993")))
                  (R0 (E0 (Left "o_orderdate")))))
              (P0
                (R2
                  RLe
                  (R0 (E0 (Left "o_orderdate")))
                  (R0
                    (E0
                      (Right "1996")))))))
          (P0
            (R2 REq
                (R0 (E0 (Left "p_type")))
                (R0 (E0 (Right "PROMO POLISHED TIN"))))))
        (Q2
          QProd
          (Q2
            QProd
            (Q2
              QProd
              (Q2
                QProd
                (Q2
                  QProd
                  (Q2
                    QProd
                    (Q2 QProd
                        (Q0 ["p_partkey","p_type"])
                        (Q0 ["s_suppkey","s_nationkey"]))
                    (Q0 ["l_partkey","l_suppkey","l_orderkey"]))
                  (Q0 ["o_orderkey","o_custkey","o_orderdate","o_orderdate"]))
                (Q0 ["c_custkey","c_nationkey"]))
              (Q1
                (QProj
                  [(Left "n1.n_nationkey",E0 (Left "n_nationkey")),
                   (Left "n1.n_regionkey",E0 (Left "n_regionkey"))])
                (Q0 ["n_nationkey","n_regionkey","n_nationkey","n_name"])))
            (Q1
              (QProj
                [(Left "n2.n_nationkey",E0 (Left "n_nationkey")),
                 (Left "n2.n_name",E0 (Left "n_name"))])
              (Q0 ["n_nationkey","n_regionkey","n_nationkey","n_name"])))
          (Q0 ["r_regionkey","r_name"])))
#endif
