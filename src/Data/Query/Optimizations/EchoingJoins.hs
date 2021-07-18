{-# LANGUAGE PatternSynonyms #-}
module Data.Query.Optimizations.EchoingJoins () where


import           Control.Applicative
import           Control.Monad.Identity
import           Control.Monad.State
import           Control.Utils.Free
import           Data.Foldable
import           Data.Functor.Compose
import           Data.List
import qualified Data.List.NonEmpty             as NEL
import           Data.Query.Algebra
import           Data.Query.Optimizations.Echo
import           Data.Query.Optimizations.Types
import           Data.Utils.Functors
import           Data.Utils.ListT
type L0 = []
type L1 = NEL.NonEmpty
data L2 a = L2 a (L1 a)
  deriving (Functor,Traversable,Foldable)
instance Semigroup (L2 a) where
  L2 a (a' NEL.:| as) <> bs = L2 a $ a' NEL.:| as ++ toList bs
type QId = Int

type FuzzyQuery e s = Free (Compose L1 (TQuery e)) s
wrapFuzzy :: Query e (FuzzyQuery e s) -> FuzzyQuery e s
wrapFuzzy x = FreeT $ Identity $ Free $ Compose $ return $ tunnelQuery x

pattern FuzzyQueryF a = FreeT (Identity (Free (Compose a)))
pattern FuzzyQueryP s = FreeT (Identity (Pure s))

unwrapFuzzy :: FuzzyQuery e s -> Query e0 (TQuery e0 s1)
unwrapFuzzy (FuzzyQueryF a) = _ a
unwrapFuzzy (FuzzyQueryP a) = _ a



combFuzz :: FuzzyQuery e s -> FuzzyQuery e s -> FuzzyQuery e s
combFuzz (FuzzyQueryF qs) (FuzzyQueryF qs') = FuzzyQueryF $ qs <> qs'
combFuzz (FuzzyQueryF qs) (FuzzyQueryP s) =
  FuzzyQueryF $ qs <> pure (tunnelQuery $ Q0 $ return s)
combFuzz (FuzzyQueryP s) (FuzzyQueryF qs) =
  FuzzyQueryF $ pure (tunnelQuery $ Q0 $ return s) <> qs
combFuzz (FuzzyQueryP s) (FuzzyQueryP s') =
  FuzzyQueryF
  $ pure (tunnelQuery $ Q0 $ return s)
  <> pure (tunnelQuery $ Q0 $ return s')

echoingJoins
  :: L0 (Prop (Rel (Expr (Maybe QId,e))))
  -> L1 (QId,Query e (FuzzyQuery e s))
  -> ListT (State EchoId) (FuzzyQuery e s)
echoingJoins [] ((_qid,x) NEL.:| []) = return $ wrapFuzzy x
echoingJoins (p:ps) ((_qid,x) NEL.:| []) =
  return $ wrapFuzzy $ S (fmap3 snd $ foldl And p ps) x
echoingJoins [] _ = mzero
echoingJoins ps (l0 NEL.:| r0:qs) = do
  (ls,rs) <- mkListT $ return $ foldl' onEitherSide [(pure l0,pure r0)] qs
  let lIds = fst <$> ls
      rIds = fst <$> rs
  let (eqP_,nonEqP_) = partition isEquality ps
      (lEqP,eqP__) = partition (isPushable lIds) eqP_
      (rEqP,connEqP) = partition (isPushable rIds) eqP__
      (lNonEqP,nonEqP__) = partition (isPushable lIds) nonEqP_
      (rNonEqP,connNonEqP) = partition (isPushable rIds) nonEqP__
  -- Build the tunnels.
  tid <- get
  modify (+ 1)
  let tunnelSrc q = Q0 $ TQuery { tqQuery = Left q,tqEcho = TEntry tid }
      tunnelDst q = Q0 $ TQuery { tqQuery = Left q,tqEcho = TExit tid }
  let canSel = lNonEqP <> rNonEqP <> connNonEqP
      mustJoin = connEqP
      pl = lEqP
      pr = rEqP
  let lq = wrapFuzz $ echoingJoins pl ls
      rq = wrapFuzz $ echoingJoins pr rs
  let simplestJoin = tunnelDst $ J (foldl1 And mustJoin) (Q0 rq) (Q0 lq)
  let selP = foldl1 And canSel
  let joinP = foldl1 And $ canSel <> mustJoin
  if null canSel then return simplestJoin else S selP simplestJoin
    <+> tunnelSrc (J joinP (Q0 rq) (Q0 lq))
  where
    -- <|> pushedLR (connNonEqP,connEqP) (lEqP <> lNonEqP) (rEqP <> rNonEqP)
    wrapFuzz :: ListT (State EchoId) (FuzzyQuery e s)
             -> TQuery (Maybe QId,e) s0
    wrapFuzz = error "not implemented"

(<+>) :: Alternative  m => a  -> a -> m a
a <+> b = return a <|> return b

-- Partitiion toi the following categories:
--
-- * conn : Props between l,r -- can't push use as join
-- * eqj : Equijoins not connecting
-- * pl : Pushable to left
-- * pr : Pushable to right
--
-- The rules for generating are the following:
--
-- * We always push eqj
-- * We never pish conn
-- * We non-deterministically push or don't push the [pl ++ pr]

isPushable :: L1 QId -> Prop (Rel (Expr (Maybe QId, e))) -> Bool
isPushable = error "not implemented"


isEquality :: Prop (Rel (Expr e')) -> Bool
isEquality = error "not implemented"

--
-- Finally we create echo tunnels between each of the resulting
-- queries and the pure equijoin query. This is a heuristic that
-- assumes that the particular query is useful to be considered when
-- considering andy of the queries in question.

enumerate :: L1 a -> L1 (QId,a)
enumerate (x NEL.:| xs) = (0,x) NEL.:| zip [1..] xs

onEitherSide :: L0 (L1 a,L1 a) -> a -> L0 (L1 a,L1 a)
onEitherSide res q = do
  (ls,rs) <- res
  [(pure q <> ls,rs),(ls,pure q <> rs)]
